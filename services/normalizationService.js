const { sleep } = require('./utils');

/**
 * Отправляет названия вакансий в Gemini для нормализации пакетами.
 * @param {object} supabase - Клиент Supabase.
 * @param {object} geminiModel - Клиент Gemini.
 * @param {Array} vacancies - Массив вакансий для обработки.
 */
async function normalizeTitlesInBatches(supabase, geminiModel, vacancies) {
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

/**
 * Находит все вакансии, требующие нормализации, и запускает обработку.
 * @param {object} supabase - Клиент Supabase.
 * @param {object} geminiModel - Клиент Gemini.
 */
async function normalizeAllPending(supabase, geminiModel) {
    console.log('\n--- НАЧАЛО ШАГА 2: НОРМАЛИЗАЦИЯ НАЗВАНИЙ ---');
    const { data, error } = await supabase.from('vacancies').select('id, raw_title').is('normalized_title', null).eq('status', 'active');
    if (error) throw error;
    
    if (data && data.length > 0) {
        console.log(`Найдено ${data.length} активных вакансий для нормализации.`);
        await normalizeTitlesInBatches(supabase, geminiModel, data);
    } else {
        console.log('Активных вакансий для нормализации не найдено.');
    }
}

module.exports = { 
    normalizeAllPending 
};