const axios = require('axios');
const https = require('https');

const httpsAgent = new https.Agent({
    family: 4
});

async function sendTelegramMessage(chatId, text) {
    const token = process.env.TELEGRAM_BOT_TOKEN;
    if (!token) {
        console.error('Ошибка: Переменная окружения TELEGRAM_BOT_TOKEN не задана.');
        return;
    }
    const url = `https://api.telegram.org/bot${token}/sendMessage`;
    try {
        const payload = {
            chat_id: chatId,
            text: text,
            parse_mode: 'HTML',
            disable_web_page_preview: true
        };
        await axios.post(url, payload, { httpsAgent });
    } catch (error) {
        console.error(`===== ПОДРОБНАЯ ОШИБКА AXIOS ПРИ ОТПРАВКЕ В ЧАТ ${chatId} =====`);
        console.error(error);
        console.error(`===== КОНЕЦ ПОДРОБНОЙ ОШИБКИ AXIOS =====`);
    }
}

/**
 * Формирует и отправляет сгруппированное уведомление со списком проблемных вакансий.
 * @param {string} companyId - ID компании на hh.ru
 * @param {Array} allFlawedVacancies - Массив всех новых проблемных вакансий
 * @param {object} supabase - Клиент Supabase
 */
async function sendGroupedNotifications(companyId, allFlawedVacancies, supabase) {
    if (allFlawedVacancies.length === 0) {
        return;
    }

    const { data: profiles, error } = await supabase
        .from('profiles')
        .select('telegram_chat_id, notify_no_salary, notify_no_skills, notify_no_contacts')
        .eq('company_hh_id', companyId)
        .not('telegram_chat_id', 'is', null);

    if (error || !profiles) {
        console.error("Ошибка получения профилей для уведомления:", error?.message);
        return;
    }

    for (const profile of profiles) {
        const userSpecificVacancies = allFlawedVacancies.filter(vacancy => {
            // Эта логика остается без изменений
            const hasNoSalary = vacancy.issues.includes('Не указана зарплата') && profile.notify_no_salary;
            const hasNoSkills = vacancy.issues.includes('Отсутствуют ключевые навыки') && profile.notify_no_skills;
            const hasNoContacts = vacancy.issues.includes('Скрыты контакты') && profile.notify_no_contacts;
            return hasNoSalary || hasNoSkills || hasNoContacts;
        });

        if (userSpecificVacancies.length > 0) {
            let messageText;

            // --- ИЗМЕНЕНИЯ ЗДЕСЬ ---

            // 1. Выбираем правильный заголовок (единственное или множественное число)
            if (userSpecificVacancies.length === 1) {
                messageText = `<b>Новая вакансия:</b>\n\n`;
            } else {
                messageText = `<b>Новые вакансии:</b>\n\n`;
            }

            // 2. Формируем список без точек
            userSpecificVacancies.forEach(vacancy => {
                const issuesText = vacancy.issues.join(', ').toLowerCase();
                messageText += `<a href="${vacancy.url}">${vacancy.raw_title}</a>\n`;
                messageText += `<i>(${issuesText})</i>\n\n`;
            });
            
            // --- КОНЕЦ ИЗМЕНЕНИЙ ---

            await sendTelegramMessage(profile.telegram_chat_id, messageText);
        }
    }
}

module.exports = {
    sendGroupedNotifications
};