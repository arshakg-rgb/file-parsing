import { receiveMessages } from './src/shared/queueUtils.js';
import { settings } from './src/shared/config.js';
console.log('receiving with wait 20');
const msgs = await receiveMessages(settings.PARSE_QUEUE_URL, (body) => body, 1, 20);
console.log('count:', msgs.length);
if (msgs.length) console.log(msgs[0].payload);
