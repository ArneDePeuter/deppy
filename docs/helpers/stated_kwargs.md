# StatedKwargs Documentation

`StatedKwargs` is a utility class that saves/produces kwargs for a function and persists its state. 
This is useful for example when you want to cache the state of a function and reuse it in the next function call outside the runtime.

## Usage

### **Initialization**

Create an instance of `StatedKwargs`, specifying the file where the cached state should be saved:

```python
from deppy.helpers.wrappers.stated_kwargs import StatedKwargs
stated_kwargs = StatedKwargs(state_file="my_state.json")
```

you can add a stated_kwargs by calling `stated_kwargs.stated_kwargs` method:
which takes the following arguments:
- `name`: the name of the kwarg
- `produce_function`: a function that produces the value of the kwarg
- `initial_value`: the initial value of the kwarg
- `from_result`: if True, the result will be passed to the produce_function
- `from_prev_state`: if True, the previous state will be passed to the produce_function
- `keys`: a list of keys that identifies the state of the kwargs. Derived from the kwargs passed to the original function.

This method will return a wrapper you can wrap around your original function. Or use it as a decorator.

