import express from 'express';
import { Broker, healthHandler } from '../shared/broker.mjs';

const broker = new Broker('orchestrator');
broker.consume('saga.queue', (msg, ch) => ch.ack(msg), Number(process.env.SAGA_CONCURRENCY || 5)); // step 2 replaces this
const app = express();
app.get('/health', healthHandler(broker));
app.listen(Number(process.env.PORT), () => console.log('[orchestrator] listening'));
broker.start();
