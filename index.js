require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// --- ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ ---
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const geminiModel = genAI.getGenerativeModel({ model: "gemini-1.5-flash"});
const HH_API_URL = 'https://api.hh.ru/vacancies';
const USER_AGENT = process.env.HH_USER_AGENT || 'analyzer-script/1.0';

/**
 * Главная функция, запускающая все этапы работы скрипта.
 */
async function main() {
  console.log('Скрипт запущен...');
  try {
    await syncAllCompanies();
    await normalizeAllPending();
    await trackPositionsAndCompetitorsTransactional();
    console.log('\nСкрипт успешно завершил работу!');
  } catch (error) {
    console.error('КРИТИЧЕСКАЯ ОШИБКА В main:', error.message);
  }
}

// --- ШАГ 1: СИНХРОНИЗАЦИЯ ВАКАНСИЙ С HH.RU ---

/**
 * Запускает процесс синхронизации для всех компаний из профилей.
 */
async function syncAllCompanies() {
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
      await syncVacanciesInDB(companyId, fetchedVacancies);
    }
  }
}

/**
 * Загружает все страницы с активными вакансиями для одной компании.
 * @param {string} companyId - ID компании на hh.ru.
 * @returns {Promise<Array>} - Массив объектов вакансий.
 */
async function fetchAllVacanciesForCompany(companyId) {
    let allVacancies = [];
    let page = 0;
    const perPage = 100;
    while (true) {
        try {
            const response = await axios.get(HH_API_URL, {
                params: { employer_id: companyId, per_page: perPage, page: page, archived: false },
                headers: { 'User-Agent': USER_AGENT },
            });
            const items = response.data.items;
            if (items.length === 0) break;
            
            allVacancies = allVacancies.concat(items);
            page++;
            if (response.data.pages === page) break;
        } catch (error) {
            console.error(`Ошибка при получении вакансий для компании ${companyId}, стр. ${page}:`, error.message);
            break;
        }
    }
    return allVacancies;
}

/**
 * Синхронизирует вакансии в базе данных: добавляет новые, закрывает старые
 * и проверяет изменения в названиях существующих.
 * @param {string} companyId - ID компании на hh.ru.
 * @param {Array} fetchedVacancies - Массив вакансий, полученных с hh.ru.
 */
async function syncVacanciesInDB(companyId, fetchedVacancies) {
    const { data: existingActiveVacancies, error: existingError } = await supabase
        .from('vacancies')
        .select('id, hh_vacancy_id, raw_title')
        .eq('company_hh_id', companyId)
        .eq('status', 'active');

    if (existingError) throw new Error(`Ошибка получения существующих вакансий: ${existingError.message}`);

    const existingVacanciesMap = new Map(existingActiveVacancies.map(v => [v.hh_vacancy_id, { id: v.id, raw_title: v.raw_title }]));
    const fetchedVacancyIds = new Set(fetchedVacancies.map(v => parseInt(v.id)));

    // Поиск новых вакансий для добавления
    const newVacanciesToInsert = fetchedVacancies
        .filter(v => !existingVacanciesMap.has(parseInt(v.id)))
        .map(v => ({
            company_hh_id: companyId, hh_vacancy_id: parseInt(v.id),
            raw_title: v.name, area_name: v.area.name, area_id: parseInt(v.area.id),
            schedule_id: v.schedule.id, url: v.alternate_url, status: 'active',
            published_at: v.published_at, salary_from: v.salary ? v.salary.from : null,
            salary_to: v.salary ? v.salary.to : null, salary_currency: v.salary ? v.salary.currency : null,
            salary_gross: v.salary ? v.salary.gross : null,
        }));
    
    if (newVacanciesToInsert.length > 0) {
        console.log(`Добавление ${newVacanciesToInsert.length} новых вакансий...`);
        const { error } = await supabase.from('vacancies').insert(newVacanciesToInsert);
        if (error) console.error('Ошибка добавления новых вакансий:', error.message);
    } else {
        console.log('Новых вакансий для добавления нет.');
    }

    // Поиск закрытых вакансий для обновления статуса
    const closedVacancyIds = [...existingVacanciesMap.keys()].filter(id => !fetchedVacancyIds.has(id));
    if (closedVacancyIds.length > 0) {
        console.log(`Обновление ${closedVacancyIds.length} закрытых вакансий...`);
        const { error } = await supabase.from('vacancies').update({ status: 'closed' }).in('hh_vacancy_id', closedVacancyIds);
        if (error) console.error('Ошибка обновления статуса закрытых вакансий:', error.message);
    } else {
        console.log('Закрытых вакансий для обновления нет.');
    }

    // Проверка изменений в названии у существующих вакансий
    const vacanciesWithChangedTitle = [];
    for (const fetchedV of fetchedVacancies) {
        const hhId = parseInt(fetchedV.id);
        if (existingVacanciesMap.has(hhId)) {
            const existingV = existingVacanciesMap.get(hhId);
            if (existingV.raw_title !== fetchedV.name) {
                console.log(` -> Название изменено: было "${existingV.raw_title}", стало "${fetchedV.name}"`);
                vacanciesWithChangedTitle.push({
                    id: existingV.id, raw_title: fetchedV.name,
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

// --- ШАГ 2: НОРМАЛИЗАЦИЯ НАЗВАНИЙ ЧЕРЕЗ AI ---

/**
 * Находит все вакансии, требующие нормализации, и запускает обработку.
 */
async function normalizeAllPending() {
  console.log('\n--- НАЧАЛО ШАГА 2: НОРМАЛИЗАЦИЯ НАЗВАНИЙ ---');
  const { data, error } = await supabase.from('vacancies').select('id, raw_title').is('normalized_title', null).eq('status', 'active');
  if (error) throw error;
  
  if (data && data.length > 0) {
    console.log(`Найдено ${data.length} активных вакансий для нормализации.`);
    await normalizeTitlesInBatches(data);
  } else {
    console.log('Активных вакансий для нормализации не найдено.');
  }
}

/**
 * Отправляет названия вакансий в Gemini для нормализации пакетами.
 * @param {Array} vacancies - Массив вакансий для обработки.
 */
async function normalizeTitlesInBatches(vacancies) {
    const BATCH_SIZE = 150;
    for (let i = 0; i < vacancies.length; i += BATCH_SIZE) {
        const batch = vacancies.slice(i, i + BATCH_SIZE);
        console.log(`\nОбработка партии ${Math.floor(i / BATCH_SIZE) + 1} (вакансии с ${i + 1} по ${i + batch.length})...`);
        const titlesToProcess = batch.map(v => ({ id: v.id, title: v.raw_title }));
        const prompt = `Твоя задача - максимально агрессивно нормализовать названия вакансий, оставив только самую суть профессии. Правила: 1. Удаляй уровни должностей. 2. Удаляй уточнения в скобках. 3. Если несколько должностей через слэш (/), оставляй первую. 4. Убирай лишние специализации. 5. Сокращай длинные названия. Примеры: "Монтажник РЭА и приборов" -> "Монтажник РЭА", "Токарь на оборонный завод" -> "Токарь", "Ведущий (старший) бухгалтер" -> "Бухгалтер", "Казначей/финансовый менеджер" -> "Казначей", "Наладчик станков и манипуляторов с программным управлением" -> "Наладчик станков", "Подручный (помощник станочника)" -> "Подручный", "Токарь-карусельщик / расточник" -> "Токарь-карусельщик". КРАЙНЕ ВАЖНО: Твой ответ должен быть только и исключительно валидным JSON-массивом объектов, где каждый объект имеет вид {"id": "uuid_вакансии", "title": "нормализованное_название"}. Не добавляй ничего лишнего. Вот список: ${JSON.stringify(titlesToProcess)}`;
        
        let text = '';
        try {
            const result = await geminiModel.generateContent(prompt);
            const response = result.response; 
            text = response.text();
            
            const jsonMatch = text.match(/\[[\s\S]*\]/);
            if (!jsonMatch) throw new Error("В ответе от Gemini не найден JSON-массив.");
            
            const cleanedJsonString = jsonMatch[0];
            const normalizedDataArray = JSON.parse(cleanedJsonString);
            console.log(`Gemini успешно обработал ${normalizedDataArray.length} названий.`);
            
            const updates = normalizedDataArray.map(item => supabase.from('vacancies').update({ normalized_title: item.title }).eq('id', item.id));
            await Promise.all(updates);
            console.log('Нормализованные названия сохранены в базе.');
        } catch (error) {
            console.error(`\n!!! Ошибка при обработке партии с Gemini: ${error.message}`);
            console.error("--- НАЧАЛО ПРОБЛЕМНОГО ОТВЕТА ОТ GEMINI ---\n"); console.error(text);
            console.error("--- КОНЕЦ ПРОБЛЕМНОГО ОТВETA OT GEMINI ---\n");
        }
        
        if (i + BATCH_SIZE < vacancies.length) {
            const pauseDuration = 20000; // Увеличена пауза для соблюдения лимитов free-tier Gemini (3 запроса в минуту)
            console.log(`Пауза ${pauseDuration / 1000} секунд перед следующей партией...`);
            await sleep(pauseDuration);
        }
    }
}

// --- ШАГ 3: ОПТИМИЗИРОВАННОЕ ОТСЛЕЖИВАНИЕ ПОЗИЦИЙ ---

/**
 * Отслеживает позиции вакансий, группируя запросы для повышения эффективности.
 */
async function trackPositionsAndCompetitorsTransactional() {
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

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

/**
 * Выполняет axios-запрос с несколькими попытками в случае сбоя.
 * @param {object} axiosConfig - Конфигурация для запроса axios.
 * @param {number} retries - Количество попыток.
 * @param {number} baseDelay - Начальная задержка перед повтором.
 * @returns {Promise<object>} - Ответ от сервера.
 */
async function makeRequestWithRetries(axiosConfig, retries = 5, baseDelay = 2000) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const response = await axios(axiosConfig);
            return response;
        } catch (error) {
            const isRetryable = error.response && [403, 429, 500, 502, 503, 504].includes(error.response.status);
            if (isRetryable && attempt < retries) {
                const delay = baseDelay * (2 ** (attempt - 1));
                console.warn(` -> Попытка ${attempt} не удалась (${error.response.status}). Повтор через ${delay / 1000} сек...`);
                await sleep(delay);
            } else {
                console.error(` -> Финальная ошибка после ${attempt} попыток.`, error.response ? error.response.status : error.message);
                throw error;
            }
        }
    }
}

/**
 * Функция-пауза.
 * @param {number} ms - Время в миллисекундах.
 */
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }


// --- ЗАПУСК СКРИПТА ---
main();