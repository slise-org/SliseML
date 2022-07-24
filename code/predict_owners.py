import numpy as np
from typing import Union, List, Optional
from numbers import Number

W = [176.72621441, 0.2839978 ] #price and supply weights 
B = 407.976

def convert_to_ndarray(x: Union[Number, List[float],np.ndarray]) -> np.ndarray:
    '''
    Convert variable to the np.ndarray type

    Args:
        x: Input variable, colud be numeric, list of numerics or numpy array
    Returns:
        Converted result on numpy 1D array
    Raises:
        TypeError: Incorrect input type
        ValueError: Incorrect shape (not 1D) of inputed numpy array
    '''
    if not isinstance(x, (Number, list, np.ndarray)):
        raise TypeError('Uncorrect input type:', type(x))
    if isinstance(x, np.ndarray) and len(x.shape) != 1:
        raise ValueError('np.ndarray input should be one dimensional.')

    elif isinstance(x, Number):
        x = np.array([x])
    elif isinstance(x, List):
        x = np.array(x)
    return x

def predict_whitelist_filling(
    price: Union[float, List[float], np.ndarray], 
    supply: Union[int, List[int], np.ndarray],
    whitelist: Union[int, List[int], np.ndarray],
    penalty: Optional[float]=0.5
) -> np.ndarray:
    '''
    Predict % of whitelist filling

    Args:
        price: Selected floor price at mint stage
        supply: Selected amount of supplied tokens at mint stage
        whitelist: Number of occupied places in whitelist
        penalty: Penalty assuming the share of whitelist participants 
            who redeem tokens
    Returns:
        Predicted share of purchased tokens on the mint stage.
    Raises:
        ValueError: inconsistent inputs lengths.
    '''
    price = convert_to_ndarray(price)
    whitelist = convert_to_ndarray(whitelist)
    supply = convert_to_ndarray(supply)

    if any(price < 0) or any(whitelist < 0) or any(supply < 0):
        raise ValueError('Incorrect input: negative input value')
    
    if any(whitelist > supply):
        raise ValueError('Incorrect input: whitelist members amount are bigger than supply')

    if not len(price)==len(whitelist)==len(whitelist):
        raise ValueError('Input variables should be the same length!')

    preds = (whitelist * penalty)/(W[0] * 100 * price + W[1] * supply + B)

    # Edge cases 
    # Extreme values 
    preds = np.where(preds > 0, preds, 0)
    preds = np.where(preds < 1, preds, 1)

    # Free tokens. Everyone will take them
    preds = np.where(price != 0, preds, 1)
    # Non-minted tokens. Can't mint them
    preds = np.where(supply != 0, preds, 0)
    return preds

