import express from 'express';
import { Broker, healthHandler } from '../shared/broker.mjs';

const broker = new Broker('observer');
broker.consume('trace.store', (msg, ch) => ch.ack(msg)); // step 5 replaces this
const app = express();
app.get('/health', healthHandler(broker));
app.listen(Number(process.env.PORT), () => console.log('[observer] listening'));
broker.start();
