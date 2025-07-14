const { fetchAllVacanciesForCompany } = require('./hhService');

/**
 * Улучшенная версия. Синхронизирует вакансии: добавляет новые, реактивирует старые,
 * закрывает отсутствующие и проверяет изменения в названиях.
 * @param {object} supabase - Клиент Supabase.
 * @param {string} companyId - ID компании на hh.ru.
 * @param {Array} fetchedVacancies - Массив вакансий, полученных с hh.ru.
 */
async function syncVacanciesInDB(supabase, companyId, fetchedVacancies) {
    // 1. Получаем ВСЕ вакансии компании из нашей БД, а не только активные
    const { data: allExistingVacancies, error: existingError } = await supabase
        .from('vacancies')
        .select('id, hh_vacancy_id, raw_title, status')
        .eq('company_hh_id', companyId);

    if (existingError) {
        throw new Error(`Ошибка получения существующих вакансий: ${existingError.message}`);
    }

    const existingVacanciesMap = new Map(allExistingVacancies.map(v => [v.hh_vacancy_id, { id: v.id, raw_title: v.raw_title, status: v.status }]));
    const fetchedVacancyIds = new Set(fetchedVacancies.map(v => parseInt(v.id)));

    // 2. Поиск абсолютно НОВЫХ вакансий для добавления (INSERT)
    const newVacanciesToInsert = fetchedVacancies
        .filter(v => !existingVacanciesMap.has(parseInt(v.id)))
        .map(v => ({
            company_hh_id: companyId,
            hh_vacancy_id: parseInt(v.id),
            raw_title: v.name,
            area_name: v.area.name,
            area_id: parseInt(v.area.id),
            schedule_id: v.schedule.id,
            url: v.alternate_url,
            status: 'active',
            published_at: v.published_at,
            salary_from: v.salary ? v.salary.from : null,
            salary_to: v.salary ? v.salary.to : null,
            salary_currency: v.salary ? v.salary.currency : null,
            salary_gross: v.salary ? v.salary.gross : null,
        }));

    if (newVacanciesToInsert.length > 0) {
        console.log(`Добавление ${newVacanciesToInsert.length} новых вакансий...`);
        const { error } = await supabase.from('vacancies').insert(newVacanciesToInsert);
        if (error) console.error('Ошибка добавления новых вакансий:', error.message);
    } else {
        console.log('Новых вакансий для добавления нет.');
    }

    // 3. Поиск вакансий для РЕАКТИВАЦИИ (UPDATE)
    // Это те, что есть на hh.ru, есть у нас в базе, но имеют статус 'closed'
    const vacanciesToReactivateIds = allExistingVacancies
        .filter(v => v.status === 'closed' && fetchedVacancyIds.has(v.hh_vacancy_id))
        .map(v => v.id);

    if (vacanciesToReactivateIds.length > 0) {
        console.log(`Реактивация ${vacanciesToReactivateIds.length} ранее закрытых вакансий...`);
        const { error } = await supabase.from('vacancies').update({ status: 'active' }).in('id', vacanciesToReactivateIds);
        if (error) console.error('Ошибка реактивации вакансий:', error.message);
    }

    // 4. Поиск ЗАКРЫТЫХ вакансий для обновления статуса
    // Это те, что были 'active' у нас, но их больше нет на hh.ru
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

    // 5. Проверка изменений в названии у существующих вакансий
    const vacanciesWithChangedTitle = [];
    for (const fetchedV of fetchedVacancies) {
        const hhId = parseInt(fetchedV.id);
        if (existingVacanciesMap.has(hhId)) {
            const existingV = existingVacanciesMap.get(hhId);
            if (existingV.raw_title !== fetchedV.name) {
                console.log(` -> Название изменено: было "${existingV.raw_title}", стало "${fetchedV.name}"`);
                vacanciesWithChangedTitle.push({
                    id: existingV.id,
                    raw_title: fetchedV.name,
                    normalized_title: null // Сброс для повторной нормализации!
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
        const results = await Promise.allSettled(updatePromises);
        results.forEach((result, index) => {
            if (result.status === 'rejected') {
                console.error(`Ошибка обновления вакансии ${vacanciesWithChangedTitle[index].id}:`, result.reason.message);
            }
        });
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

    const companyIds = [...new Set(profiles.map(p => p.company_hh_id))];
    console.log(`Найдено ${companyIds.length} уникальных компаний.`);

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