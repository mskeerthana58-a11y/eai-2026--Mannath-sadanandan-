import express from 'express';
import { Broker, healthHandler } from '../shared/broker.mjs';

const broker = new Broker('notifier');
broker.consume('notifications.queue', (msg, ch) => ch.ack(msg)); // step 4 replaces this
const app = express();
app.get('/health', healthHandler(broker));
app.listen(Number(process.env.PORT), () => console.log('[notifier] listening'));
broker.start();
