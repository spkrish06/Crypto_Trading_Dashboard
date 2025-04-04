#include <iostream>
#include <cpr/cpr.h>
#include <json/json.h>

void fetchMarketData() {
    std::string url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT";
    cpr::Response r = cpr::Get(cpr::Url{url});
    
    Json::Value jsonData;
    Json::CharReaderBuilder reader;
    std::string errs;
    std::istringstream ss(r.text);
    if (!Json::parseFromStream(reader, ss, &jsonData, &errs)) {
        std::cerr << "Error parsing JSON: " << errs << std::endl;
        return;
    }
    
    std::cout << "Bitcoin Price: " << jsonData["price"].asString() << std::endl;
}

int main() {
    fetchMarketData();
    return 0;
}
