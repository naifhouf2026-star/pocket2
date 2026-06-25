import asyncio
from fastapi import FastAPI, Query
import uvicorn
from pocketoptionapi_async import AsyncPocketOptionClient

class Colors:
    GREEN = '\033[92m'; RED = '\033[91m'; BLUE = '\033[94m'
    YELLOW = '\033[93m'; CYAN = '\033[96m'; BOLD = '\033[1m'
    DIM = '\033[2m'; RESET = '\033[0m'

app = FastAPI()

SSID = '42["auth",{"session":"c6v74skiu8l58ls0k2iesll1fa","isDemo":1,"uid":71923919,"platform":2}]'

# الفريمات المدعومة (بالدقايق)، السيرفر هيجلب بياناتها كلها
SUPPORTED_TIMEFRAMES = [1, 5, 15] 
HISTORY_COUNT = 300
ASSETS_TO_STREAM = [
    "100GBP_otc", "#AAPL_otc", "#AXP_otc", "#BA_otc", "#CSCO_otc", 
    "#FB_otc", "#INTC_otc", "#JNJ_otc", "#MCD_otc", "#MSFT_otc", 
    "#PFE_otc", "#TSLA_otc", "#XOM_otc", "ADA-USD_otc", "AEDCNY_otc", 
    "AMD_otc", "AMZN_otc", "AUDCAD_otc", "AUDCHF_otc", "AUDJPY_otc", 
    "AUDNZD_otc", "AUDUSD_otc", "AUS200_otc", "AVAX_otc", "BABA_otc", 
    "BHDCNY_otc", "BITB_otc", "BNB-USD_otc", "BTCUSD_otc", "CADCHF_otc", 
    "CADJPY_otc", "CHFJPY_otc", "CHFNOK_otc", "CITI_otc", "COIN_otc", 
    "D30EUR_otc", "DJI30_otc", "DOGE_otc", "DOTUSD_otc", "E35EUR_otc", 
    "E50EUR_otc", "ETHUSD_otc", "EURAUD_otc", "EURCAD_otc", "EURCHF_otc", 
    "EURGBP_otc", "EURHUF_otc", "EURJPY_otc", "EURNZD_otc", "EURRUB_otc", 
    "EURTRY_otc", "EURUSD_otc", "F40EUR_otc", "FDX_otc", "GBPAUD_otc", 
    "GBPCAD_otc", "GBPCHF_otc", "GBPJPY_otc", "GBPNZD_otc", "GBPUSD_otc", 
    "GME_otc", "IRRUSD_otc", "JODCNY_otc", "JPN225_otc", "KESUSD_otc", 
    "LBPUSD_otc", "LINK_otc", "LTCUSD_otc", "MADUSD_otc", "MARA_otc", 
    "MATIC_otc", "NASUSD_otc", "NFLX_otc", "NGNUSD_otc", "NZDJPY_otc", 
    "NZDUSD_otc", "OMRCNY_otc", "PLTR_otc", "QARCNY_otc", "SARCNY_otc", 
    "SOL-USD_otc", "SP500_otc", "SYPUSD_otc", "TNDUSD_otc", "TON-USD_otc", 
    "TRX-USD_otc", "UAHUSD_otc", "UKBrent_otc", "USCrude_otc", "USDARS_otc", 
    "USDBDT_otc", "USDBRL_otc", "USDCAD_otc", "USDCHF_otc", "USDCLP_otc", 
    "USDCNH_otc", "USDCOP_otc", "USDDZD_otc", "USDEGP_otc", "USDIDR_otc", 
    "USDINR_otc", "USDJPY_otc", "USDMXN_otc", "USDMYR_otc", "USDPHP_otc", 
    "USDPKR_otc", "USDRUB_otc", "USDSGD_otc", "USDTHB_otc", "USDVND_otc", 
    "VISA_otc", "VIX_otc", "XAGUSD_otc", "XAUUSD_otc", "XNGUSD_otc", 
    "XPDUSD_otc", "XPTUSD_otc", "YERUSD_otc", "ZARUSD_otc"
]

# قاموس لتخزين الشموع بناءً على الفريم
mt4_assets = {tf: {} for tf in SUPPORTED_TIMEFRAMES}

class MT4Asset:
    def __init__(self, symbol):
        self.symbol = symbol
        self.candles = []
        self.last_tick_close = 0.0
        self.last_tick_vol = 0

async def po_data_fetcher(tf_minutes: int):
    tf_seconds = tf_minutes * 60  # تحويل الدقايق لثواني لمنصة PO
    client = AsyncPocketOptionClient(SSID, is_demo=True, enable_logging=False)
    await client.connect()
    print(f"{Colors.GREEN}✅ Connected to PO Server for M{tf_minutes}.{Colors.RESET}")

    print(f"{Colors.YELLOW}⏳ Fetching history for M{tf_minutes}...{Colors.RESET}")
    for asset in ASSETS_TO_STREAM:
        asset_obj = MT4Asset(asset)
        try:
            df = await client.get_candles_dataframe(asset=asset, timeframe=tf_seconds, count=HISTORY_COUNT)
            if df is not None and not df.empty:
                for index, row in df.iterrows():
                    ts = int(index.timestamp())
                    asset_obj.candles.append({
                        'time': ts, 'open': row['open'], 'high': row['high'],
                        'low': row['low'], 'close': row['close'], 'volume': row['volume']
                    })
                mt4_assets[tf_minutes][asset] = asset_obj
                print(f"{Colors.GREEN}[M{tf_minutes}] [{asset:<12}] Saved ({len(df)} candles){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}Error loading {asset} (M{tf_minutes}): {e}{Colors.RESET}")
        await asyncio.sleep(1)

    while True:
        try:
            for asset_name, asset_obj in mt4_assets[tf_minutes].items():
                live = await client.get_candles_live3(asset=asset_name, timeframe=tf_seconds)
                if not live: continue

                live_close = float(live['close'])
                live_vol = int(live['volume'])

                if asset_obj.last_tick_close > 0:
                    price_diff = abs(live_close - asset_obj.last_tick_close) / asset_obj.last_tick_close
                    if price_diff > 0.01: continue

                if live_close != asset_obj.last_tick_close or live_vol != asset_obj.last_tick_vol:
                    last_c = asset_obj.candles[-1]
                    
                    if live_vol < last_c['volume'] and live_vol < 10:
                        new_ts = last_c['time'] + tf_seconds
                        asset_obj.candles.append({
                            'time': new_ts, 'open': float(live['open']),
                            'high': float(live['high']), 'low': float(live['low']),
                            'close': float(live['close']), 'volume': live_vol
                        })
                        if len(asset_obj.candles) > HISTORY_COUNT:
                            asset_obj.candles.pop(0)
                        print(f"{Colors.CYAN}[M{tf_minutes}] [{asset_name:<12}] 🟩 New candle | Price: {live_close}{Colors.RESET}")
                    else:
                        last_c['high'] = max(last_c['high'], float(live['high']))
                        last_c['low'] = min(last_c['low'], float(live['low']))
                        last_c['close'] = live_close
                        last_c['volume'] = live_vol

                asset_obj.last_tick_close = live_close
                asset_obj.last_tick_vol = live_vol
        except Exception as e:
            pass
        await asyncio.sleep(0.1)

@app.on_event("startup")
async def startup_event():
    # تشغيل متزامن لكل الفريمات المدعومة
    for tf in SUPPORTED_TIMEFRAMES:
        asyncio.create_task(po_data_fetcher(tf))

# الـ Endpoint بيستقبل الـ tf (تايم فريم) ويرجع بياناته
@app.get("/")
async def get_all_ticks(tf: int = 1):
    if tf not in mt4_assets:
        return ""
    
    result = []
    for asset, obj in mt4_assets[tf].items():
        if obj.candles:
            candles_str = [asset]
            for c in obj.candles:
                candles_str.append(f"{c['time']},{c['open']},{c['high']},{c['low']},{c['close']},{c['volume']}")
            result.append(",".join(candles_str))
            
    return ";".join(result)

if __name__ == "__main__":
    print(f"{Colors.BOLD}🚀 NAIF SaaS Server Started (Localhost){Colors.RESET}")
    uvicorn.run(app, host="127.0.0.1", port=8000)
