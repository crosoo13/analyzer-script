const axios = require('axios');

/**
 * Функция-пауза.
 * @param {number} ms - Время в миллисекундах.
 */
function sleep(ms) { 
    return new Promise(resolve => setTimeout(resolve, ms)); 
}

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
                console.error(` -> Финальная ошибка после ${attempt} попыток для URL: ${axiosConfig.url}`);
                if (error.response) {
                    console.error(` -> Статус ответа: ${error.response.status}`);
                    console.error(` -> Тело ответа:`, error.response.data);
                } else {
                    console.error(` -> Ошибка без ответа от сервера: ${error.message}`);
                }
                throw error;
            }
        }
    }
}

// Убедитесь, что обе функции экспортируются
module.exports = {
    sleep,
    makeRequestWithRetries
};