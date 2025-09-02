const { fetchAllVacanciesForCompany } = require('./hhService');
const { sendGroupedNotifications } = require('./notificationService');
const { sleep, makeRequestWithRetries } = require('./utils');

const USER_AGENT = process.env.HH_USER_AGENT || 'analyzer-script/1.0';

/**
 * Получает ВСЕ записи из таблицы Supabase, обходя ограничение в 1000 строк.
 * @param {object} query - Начальный запрос Supabase (e.g., supabase.from('...').select('...')).
 * @returns {Promise<Array>} - Полный массив данных.
 */
async function fetchAllSupabasePages(query) {
    let allData = [];
    let page = 0;
    const pageSize = 1000; // Стандартный лимит Supabase на один запрос

    while (true) {
        const { data, error } = await query.range(page * pageSize, (page + 1) * pageSize - 1);

        if (error) {
            console.error("Ошибка при постраничной загрузке из Supabase:", error.message);
            throw error;
        }

        if (data && data.length > 0) {
            allData.push(...data);
        }

        // Если данных вернулось меньше, чем мы запрашивали, это последняя страница
        if (!data || data.length < pageSize) {
            break;
        }

        page++;
    }
    return allData;
}


/**
 * Синхронизирует вакансии: добавляет новые (с уведомлениями), реактивирует старые,
 * закрывает отсутствующие и проверяет изменения в названиях.
 * @param {object} supabase - Клиент Supabase.
 * @param {string} companyId - ID компании на hh.ru.
 * @param {Array} fetchedVacancies - Массив вакансий, полученных с hh.ru.
 */
async function syncVacanciesInDB(supabase, companyId, fetchedVacancies) {
    // 1. Получаем ВСЕ вакансии компании из нашей БД с помощью пагинации
    console.log(`Получение всех существующих вакансий из БД для компании ${companyId}...`);
    const query = supabase
        .from('vacancies')
        .select('id, hh_vacancy_id, raw_title, status')
        .eq('company_hh_id', companyId);

    const allExistingVacancies = await fetchAllSupabasePages(query);
    console.log(`Всего в базе найдено ${allExistingVacancies.length} записей для этой компании.`);


    // 2. Определяем, является ли синхронизация начальной (нет активных вакансий в базе)
    const hasActiveVacanciesInDB = allExistingVacancies.some(v => v.status === 'active');
    const isInitialSync = !hasActiveVacanciesInDB;

    if (isInitialSync) {
        console.log(`[Начальная синхронизация] для компании ${companyId}. Уведомления отключены для этой партии.`);
    }

    const existingVacanciesMap = new Map(allExistingVacancies.map(v => [v.hh_vacancy_id, { id: v.id, raw_title: v.raw_title, status: v.status }]));
    const fetchedVacancyIds = new Set(fetchedVacancies.map(v => parseInt(v.id)));

    // 3. Обработка НОВЫХ вакансий
    const newVacanciesSummaries = fetchedVacancies.filter(v => !existingVacanciesMap.has(parseInt(v.id)));

    if (newVacanciesSummaries.length > 0) {
        if (isInitialSync) {
            // --- РЕЖИМ НАЧАЛЬНОЙ СИНХРОНИЗАЦИИ (БЕЗ УВЕДОМЛЕНИЙ) ---
            console.log(`Добавление ${newVacanciesSummaries.length} стартовых вакансий...`);
            const initialVacanciesToInsert = newVacanciesSummaries.map(summary => ({
                company_hh_id: companyId,
                hh_vacancy_id: parseInt(summary.id),
                raw_title: summary.name,
                area_name: summary.area.name,
                area_id: parseInt(summary.area.id),
                schedule_id: summary.schedule.id,
                url: summary.alternate_url,
                status: 'active',
                published_at: summary.published_at,
                salary_from: summary.salary ? summary.salary.from : null,
                salary_to: summary.salary ? summary.salary.to : null,
                salary_currency: summary.salary ? summary.salary.currency : null,
                salary_gross: summary.salary ? summary.salary.gross : null,
                show_contacts: summary.show_contacts === true,
                key_skills: [],
            }));
            const { error } = await supabase.from('vacancies').insert(initialVacanciesToInsert);
            if (error) console.error('Ошибка добавления стартовых вакансий:', error.message);
        } else {
            // --- СТАНДАРТНЫЙ РЕЖИМ (С УВЕДОМЛЕНИЯМИ) ---
            console.log(`Обнаружено ${newVacanciesSummaries.length} новых вакансий. Проверка и сбор...`);
            const vacanciesToInsert = [];
            const flawedVacanciesForGrouping = [];

            for (const summary of newVacanciesSummaries) {
                try {
                    const response = await makeRequestWithRetries({
                        method: 'get',
                        url: `https://api.hh.ru/vacancies/${summary.id}`,
                        headers: { 'User-Agent': USER_AGENT }
                    });
                    const details = response.data;

                    const fullVacancyData = {
                        company_hh_id: companyId,
                        hh_vacancy_id: parseInt(summary.id),
                        raw_title: summary.name,
                        area_name: summary.area.name,
                        area_id: parseInt(summary.area.id),
                        schedule_id: summary.schedule.id,
                        url: summary.alternate_url,
                        status: 'active',
                        published_at: summary.published_at,
                        salary_from: summary.salary ? summary.salary.from : null,
                        salary_to: summary.salary ? summary.salary.to : null,
                        salary_currency: summary.salary ? summary.salary.currency : null,
                        salary_gross: summary.salary ? summary.salary.gross : null,
                        show_contacts: summary.show_contacts === true,
                        key_skills: details.key_skills.map(s => s.name),
                    };

                    vacanciesToInsert.push(fullVacancyData);

                    // Проверяем вакансию на недостатки и собираем информацию для группового уведомления
                    const issues = [];
                    if (fullVacancyData.salary_from === null) {
                        issues.push('Не указана зарплата');
                    }
                    if (!fullVacancyData.key_skills || fullVacancyData.key_skills.length === 0) {
                        issues.push('Отсутствуют ключевые навыки');
                    }
                    if (fullVacancyData.show_contacts !== true) {
                        issues.push('Скрыты контакты');
                    }

                    if (issues.length > 0) {
                        flawedVacanciesForGrouping.push({
                            raw_title: fullVacancyData.raw_title,
                            url: fullVacancyData.url,
                            issues: issues // Сохраняем список проблем
                        });
                    }

                    await sleep(550);
                } catch (e) {
                    console.error(`Не удалось получить детали для вакансии ${summary.id} после всех попыток. Пропускаем.`);
                }
            }

            // После цикла отправляем одно сгруппированное уведомление, если есть что отправлять
            if (flawedVacanciesForGrouping.length > 0) {
                console.log(`Собрано ${flawedVacanciesForGrouping.length} проблемных вакансий. Отправка группового уведомления...`);
                await sendGroupedNotifications(companyId, flawedVacanciesForGrouping, supabase);
            }

            // И вставляем все новые вакансии в базу данных
            if (vacanciesToInsert.length > 0) {
                const { error } = await supabase.from('vacancies').insert(vacanciesToInsert);
                if (error) console.error('Ошибка добавления новых вакансий:', error.message);
            }
        }
    } else {
        console.log('Новых вакансий для добавления нет.');
    }

    // 4. Поиск вакансий для РЕАКТИВАЦИИ
    const vacanciesToReactivateIds = allExistingVacancies
        .filter(v => v.status !== 'active' && fetchedVacancyIds.has(v.hh_vacancy_id))
        .map(v => v.id);

    if (vacanciesToReactivateIds.length > 0) {
        console.log(`Реактивация ${vacanciesToReactivateIds.length} ранее закрытых вакансий...`);
        const { error } = await supabase.from('vacancies').update({ status: 'active' }).in('id', vacanciesToReactivateIds);
        if (error) console.error('Ошибка реактивации вакансий:', error.message);
    }

    // 5. Поиск ЗАКРЫТЫХ вакансий
    const closedVacancyIds = allExistingVacancies
        .filter(v => v.status === 'active' && !fetchedVacancyIds.has(v.hh_vacancy_id))
        .map(v => v.hh_vacancy_id);

    if (closedVacancyIds.length > 0) {
        console.log(`Обновление ${closedVacancyIds.length} закрытых вакансий...`);
        const { error } = await supabase.from('vacancies').update({ status: 'closed' }).in('hh_vacancy_id', closedVacancyIds);
        if (error) console.error('Ошибка обновления статуса закрытых вакансий:', error.message);
    } else {
        console.log('Активных вакансий для закрытия нет.');
    }

    // 6. Проверка изменений в названии
    const vacanciesWithChangedTitle = [];
    for (const fetchedV of fetchedVacancies) {
        const hhId = parseInt(fetchedV.id);
        if (existingVacanciesMap.has(hhId)) {
            const existingV = existingVacanciesMap.get(hhId);
            if (existingV.status === 'active' && existingV.raw_title !== fetchedV.name) {
                console.log(` -> Название изменено: было "${existingV.raw_title}", стало "${fetchedV.name}"`);
                vacanciesWithChangedTitle.push({
                    id: existingV.id,
                    raw_title: fetchedV.name,
                    normalized_title: null
                });
            }
        }
    }

    if (vacanciesWithChangedTitle.length > 0) {
        console.log(`Обнаружено ${vacanciesWithChangedTitle.length} вакансий с измененным названием. Сброс для нормализации...`);
        const updatePromises = vacanciesWithChangedTitle.map(v =>
            supabase.from('vacancies')
            .update({ raw_title: v.raw_title, normalized_title: v.normalized_title })
            .eq('id', v.id)
        );
        await Promise.all(updatePromises);
    } else {
        console.log('Изменений в названиях активных вакансий не найдено.');
    }
}


/**
 * Находит и архивирует вакансии компаний, которые были удалены из профилей.
 * @param {object} supabase - Клиент Supabase.
 */
async function archiveOrphanedVacancies(supabase) {
    console.log('\n--- ЗАПУСК ОЧИСТКИ "ОСИРОТЕВШИХ" ВАКАНСИЙ ---');

    const { data: profiles, error: profilesError } = await supabase
        .from('profiles')
        .select('company_hh_id')
        .not('company_hh_id', 'is', null);
    if (profilesError) throw profilesError;
    const validCompanyIds = new Set(profiles.map(p => p.company_hh_id));
    console.log(`Найдено ${validCompanyIds.size} актуальных компаний в профилях.`);

    const { data: activeVacancyCompanies, error: vacanciesError } = await supabase
        .from('vacancies')
        .select('company_hh_id')
        .eq('status', 'active');
    if (vacanciesError) throw vacanciesError;
    const trackedCompanyIds = new Set(activeVacancyCompanies.map(v => v.company_hh_id));
    console.log(`Найдено ${trackedCompanyIds.size} компаний с активными вакансиями в базе.`);

    const orphanedCompanyIds = [...trackedCompanyIds].filter(id => !validCompanyIds.has(id));

    if (orphanedCompanyIds.length > 0) {
        console.log(`Обнаружено ${orphanedCompanyIds.length} удаленных компаний. Архивируем их вакансии...`);
        const { error: updateError } = await supabase
            .from('vacancies')
            .update({ status: 'closed' })
            .in('company_hh_id', orphanedCompanyIds)
            .eq('status', 'active');

        if (updateError) {
            console.error('Ошибка при архивации осиротевших вакансий:', updateError.message);
        } else {
            console.log('Осиротевшие вакансии успешно заархивированы.');
        }
    } else {
        console.log('Удаленных компаний с активными вакансиями не найдено. Очистка не требуется.');
    }
}

/**
 * Запускает процесс синхронизации для всех компаний из профилей.
 * @param {object} supabase - Клиент Supabase.
 */
async function syncAllCompanies(supabase) {
    console.log('\n--- НАЧАЛО ШАГА 1: СИНХРОНИЗАЦИЯ ВАКАНСИЙ ---');
    const { data: profiles, error: profilesError } = await supabase.from('profiles').select('company_hh_id').not('company_hh_id', 'is', null);
    if (profilesError) throw profilesError;

    const companyIds = [...new Set(profiles.map(p => p.company_hh_id).filter(id => id))];
    console.log(`Найдено ${companyIds.length} уникальных компаний для синхронизации.`);

    if (companyIds.length > 0) {
        for (const companyId of companyIds) {
            console.log(`\nСинхронизация для компании с ID: ${companyId}`);
            const fetchedVacancies = await fetchAllVacanciesForCompany(companyId);
            console.log(`С HH.ru получено ${fetchedVacancies.length} активных вакансий.`);
            await syncVacanciesInDB(supabase, companyId, fetchedVacancies);
        }
    }
}

module.exports = {
    syncAllCompanies,
    archiveOrphanedVacancies
};