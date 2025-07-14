const axios = require('axios');

const HH_API_URL = 'https://api.hh.ru/vacancies';
const USER_AGENT = process.env.HH_USER_AGENT || 'analyzer-script/1.0';

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
                params: { 
                    employer_id: companyId, 
                    per_page: perPage, 
                    page: page, 
                    archived: false 
                },
                headers: { 'User-Agent': USER_AGENT },
            });
            const items = response.data.items;
            if (items.length === 0) {
                break;
            }
            
            allVacancies = allVacancies.concat(items);
            page++;
            if (response.data.pages === page) {
                break;
            }
        } catch (error) {
            console.error(`Ошибка при получении вакансий для компании ${companyId}, стр. ${page}:`, error.message);
            break;
        }
    }
    return allVacancies;
}

module.exports = {
    fetchAllVacanciesForCompany
};