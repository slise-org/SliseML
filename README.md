# slice_ml

### Environment setup
Tested on python=3.8.13
```
conda create -n slice python=3.8
conda activate slice
pip install -r requirements.txt
```

`requirements.dev.txt` additionaly contanis `pytest` package.


### Running FastAPI app locally
```
uvicorn main:app --app-dir \code
```
Test API: http://127.0.0.1:8000/docs