const { makeRequestWithRetries, sleep } = require('./utils');

const HH_API_URL = 'https://api.hh.ru/vacancies';
const USER_AGENT = process.env.HH_USER_AGENT || 'analyzer-script/1.0';

/**
 * Отслеживает позиции вакансий, группируя запросы для повышения эффективности.
 * @param {object} supabase - Клиент Supabase.
 */
async function trackPositionsAndCompetitorsTransactional(supabase) {
    console.log('\n--- НАЧАЛО ШАГА 3: ОТСЛЕЖИВАНИЕ ПОЗИЦИЙ (ОПТИМИЗИРОВАННЫЙ РЕЖИМ) ---');
    
    const { data: vacancies, error: vacanciesError } = await supabase
        .from('vacancies')
        .select('id, hh_vacancy_id, normalized_title, area_id, schedule_id')
        .not('normalized_title', 'is', null)
        .eq('status', 'active');

    if (vacanciesError) throw vacanciesError;
    if (!vacancies || vacancies.length === 0) {
        console.log('Нет активных вакансий для отслеживания.');
        return;
    }

    const groupedVacancies = new Map();
    for (const vacancy of vacancies) {
        const groupKey = `${vacancy.normalized_title}_${vacancy.area_id}_${vacancy.schedule_id}`;
        if (!groupedVacancies.has(groupKey)) {
            groupedVacancies.set(groupKey, []);
        }
        groupedVacancies.get(groupKey).push(vacancy);
    }
    console.log(`Сформировано ${groupedVacancies.size} уникальных поисковых групп из ${vacancies.length} вакансий.`);

    const { data: reportData, error: reportError } = await supabase
        .from('reports')
        .insert({ status: 'pending', total_vacancies: vacancies.length })
        .select('id').single();

    if (reportError) throw new Error(`Не удалось создать запись отчета: ${reportError.message}`);
    const currentReportId = reportData.id;
    console.log(`Отчет создан с ID: ${currentReportId}. Начинаю обработку групп...`);

    const allPositionReports = [];
    let processedCount = 0;

    try {
        let groupIndex = 0;
        for (const vacancyGroup of groupedVacancies.values()) {
            groupIndex++;
            const representative = vacancyGroup[0];
            
            console.log(`\n[Группа ${groupIndex}/${groupedVacancies.size}] Поиск для "${representative.normalized_title}" (вакансий в группе: ${vacancyGroup.length})`);

            const axiosConfig = {
                method: 'get', url: HH_API_URL,
                params: {
                    text: representative.normalized_title, area: representative.area_id,
                    schedule: representative.schedule_id, order_by: 'relevance',
                    per_page: 100, page: 0,
                },
                headers: { 'User-Agent': USER_AGENT },
            };

            try {
                const response = await makeRequestWithRetries(axiosConfig);
                const competitors_count = response.data.found;
                console.log(` -> Найдено конкурентов: ${competitors_count}`);

                const positionMap = new Map(response.data.items.map((item, index) => [parseInt(item.id), index + 1]));

                for (const vacancy of vacancyGroup) {
                    let position;
                    if (positionMap.has(vacancy.hh_vacancy_id)) {
                        position = positionMap.get(vacancy.hh_vacancy_id);
                    } else {
                        position = response.data.pages > 0 ? 100 : 'Не найдено';
                    }
                    
                    console.log(`  - Вакансия ${vacancy.hh_vacancy_id}: Позиция ${position}`);
                    
                    allPositionReports.push({
                        report_id: currentReportId, vacancy_id: vacancy.id,
                        position: position, competitors_count: competitors_count,
                    });
                }
            } catch (searchError) {
                console.error(` -> !!! Ошибка поиска для группы "${representative.normalized_title}". Пропускаем группу.`);
                for (const vacancy of vacancyGroup) {
                    allPositionReports.push({
                        report_id: currentReportId, vacancy_id: vacancy.id,
                        position: 'Ошибка', competitors_count: 0,
                        error_message: `Не удалось получить данные для группы после всех попыток.`
                    });
                }
            }
            
            if (groupIndex < groupedVacancies.size) await sleep(500); 
        }
        
        processedCount = allPositionReports.length;
        console.log(`\nОбработка всех групп завершена. Сохранение ${processedCount} отчетов в базу...`);
        
        if (allPositionReports.length > 0) {
            const { error: insertError } = await supabase.from('position_reports').insert(allPositionReports);
            if (insertError) throw new Error(`Ошибка массового сохранения отчетов о позициях: ${insertError.message}`);
        }

        await supabase
            .from('reports')
            .update({ status: 'completed', processed_vacancies: processedCount, completed_at: new Date().toISOString() })
            .eq('id', currentReportId);
        
        console.log('Отчет успешно завершен!');

    } catch (error) {
        console.error('\n!!! Произошла критическая ошибка во время обработки. Отмечаем отчет как "failed"...');
        console.error('Текст ошибки:', error.message);
        await supabase
            .from('reports')
            .update({ status: 'failed', processed_vacancies: processedCount, error_message: error.message, completed_at: new Date().toISOString() })
            .eq('id', currentReportId);
        throw error;
    }
}

module.exports = {
    trackPositionsAndCompetitorsTransactional
};