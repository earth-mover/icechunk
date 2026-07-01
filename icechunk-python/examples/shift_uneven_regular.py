import numpy as np

import icechunk as ic
import zarr

repo = ic.Repository.create(storage=ic.in_memory_storage())

session = repo.writable_session("main")
root = zarr.group(store=session.store, overwrite=True)
root.create_array("array", chunks=(5,), fill_value=-1, data=np.arange(11, dtype="i4"))
session.commit("create array")
print(np.arange(11))

session = repo.writable_session("main")
session.shift_array("/array", (-1,))
session.commit("shift")

session = repo.readonly_session("main")
array = zarr.open_array(store=session.store, mode="r", path="array")
print(array[0])
print(array[-1])
for i in range(11):
    print(array[i])

print(array[:])
