from ap.brokers.tradier import TradierBroker, TradierConfig

def get_broker_for_client(client):
    return TradierBroker(
        TradierConfig(
            base_url=client["broker_base_url"],
            access_token=client["broker_token"],
            account_id=client["broker_account_id"],
        )
    )
