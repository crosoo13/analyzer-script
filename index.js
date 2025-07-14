require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// Импортируем наши модули
const { syncAllCompanies, archiveOrphanedVacancies } = require('./services/syncService');
const { normalizeAllPending } = require('./services/normalizationService');
const { trackPositionsAndCompetitorsTransactional } = require('./services/trackingService');

// --- ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ ---
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const geminiModel = genAI.getGenerativeModel({ model: "gemini-2.5-flash-lite-preview-06-17"});

/**
 * Главная функция, запускающая все этапы работы скрипта.
 */
async function main() {
    console.log('Скрипт запущен...');
    try {
        await syncAllCompanies(supabase);
        await archiveOrphanedVacancies(supabase);
        await normalizeAllPending(supabase, geminiModel);
        await trackPositionsAndCompetitorsTransactional(supabase);
        console.log('\nСкрипт успешно завершил работу!');
    } catch (error) {
        console.error('КРИТИЧЕСКАЯ ОШИБКА В main:', error.message, error.stack);
    }
}

// --- ЗАПУСК СКРИПТА ---
main();