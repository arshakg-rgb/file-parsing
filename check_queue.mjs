import { receiveMessages } from './src/shared/queueUtils.js';
import { settings } from './src/shared/config.js';
console.log('queue url:', settings.PARSE_QUEUE_URL);
const msgs = await receiveMessages(settings.PARSE_QUEUE_URL, (body) => body, 10, 0);
console.log('received count:', msgs.length);
for (const m of msgs) console.log(m.payload);
