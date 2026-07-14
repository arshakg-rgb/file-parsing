import { receiveMessages } from './src/shared/queueUtils.js';
import { settings } from './src/shared/config.js';
const messages = await receiveMessages(settings.PARSE_QUEUE_URL, (body) => JSON.parse(body), 1);
console.log('Messages:', messages.length);
for (const m of messages) console.log(m.payload);
