import express from 'express';
import { Broker, healthHandler } from '../shared/broker.mjs';

const broker = new Broker('intake');
const app = express();
app.get('/health', healthHandler(broker));
app.listen(Number(process.env.PORT), () => console.log('[intake] listening'));
broker.start();
