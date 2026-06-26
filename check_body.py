import yfinance as yf
tickers = ['VOLTAMP.NS', 'ADANIPORTS.NS', 'THERMAX.NS', 'PREMEXPLN.NS', 'STYLAMIND.NS']
data = yf.download(tickers, period='5d', progress=False)
print("Thursday body pct:")
for t in tickers:
    o = data['Open'][t].iloc[-2].item()
    c = data['Close'][t].iloc[-2].item()
    print(f"{t}: {(c-o)/o*100:.2f}%")

print("\nFriday body pct:")
for t in tickers:
    o = data['Open'][t].iloc[-1].item()
    c = data['Close'][t].iloc[-1].item()
    print(f"{t}: {(c-o)/o*100:.2f}%")
