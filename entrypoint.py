import dotenv
from src.lambda_function2 import lambda_handler2
from src.lambda_function1 import lambda_handler1

dotenv.load_dotenv()
if __name__ == "__main__":
    lambda_handler1(event=None, context=None)
    lambda_handler2(event=None, context=None)
   
