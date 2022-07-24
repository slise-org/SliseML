from predict_owners import predict_whitelist_filling

def test_zero_price():
    '''
    Edge case: free tokens
    '''
    pred = predict_whitelist_filling(0, 10000, 5000)
    assert pred == 1

def test_non_minted_contract():
    '''
    Edge case: supply = 0
    '''
    pred = predict_whitelist_filling(1, 0, 5000)
    assert pred == 0

