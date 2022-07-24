from typing import List, Optional, Dict, Union
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from predict_owners import predict_whitelist_filling
from predict_similarity import get_recs

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello World"}

@app.get("/items/")
def get_mint_share(
    price: List[float]= Query(),
    supply: List[int]= Query(),
    whitelist: List[int]= Query(),
    penalty: Optional[float]=0.5
) -> JSONResponse:
    try:
        preds = predict_whitelist_filling(price, supply, whitelist, penalty)
        preds = preds.tolist() # np.array -> list to serialize in JSONResponse
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"message": f"Fail to predict mint share. Log: {e}"})
    return JSONResponse(status_code=200,
                        content=preds)

@app.get("/recs/")
def get_similar_wallets(
    whitelist_id: str,
) -> JSONResponse:
    response = get_recs(whitelist_id)
    return response

