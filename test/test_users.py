# import os

# print(os.listdir("/Workspace/Users/aw3227548@gmail.com/ecommerce-databricks-project"))
# print(os.listdir("/Workspace/Users/aw3227548@gmail.com/ecommerce-databricks-project/src"))
# print(os.listdir("/Workspace/Users/aw3227548@gmail.com/ecommerce-databricks-project/src/bronze"))
dbutils.library.restartPython()
import src.common.utils as utils

print("USING FILE:", utils.__file__)

# 强行读源码看看
import inspect
print(inspect.getsource(utils.load_config))