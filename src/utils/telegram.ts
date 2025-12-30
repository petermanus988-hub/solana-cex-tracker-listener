import dotenv from 'dotenv';
dotenv.config();

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

if (!BOT_TOKEN || !CHAT_ID) {
  // don't throw — the bot should still run; just warn
  console.warn('TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set; alerts will be NO-OP');
}

export async function sendAlert(message: string) {
  if (!BOT_TOKEN || !CHAT_ID) return;
  try {
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`;
    // use global fetch available in Node 18+
    // eslint-disable-next-line no-undef
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: CHAT_ID, text: message, parse_mode: 'HTML' })
    });
    if (!res.ok) {
      console.warn('Telegram API error', await res.text());
    }
  } catch (err) {
    console.warn('Failed to send Telegram alert', err);
  }
}

export default { sendAlert };
