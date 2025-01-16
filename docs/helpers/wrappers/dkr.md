# DKR

This module provides utilities for handling dynamic keyword arguments (DKs), allowing placeholders in strings, mappings, and iterables to be dynamically resolved based on provided data. The `Dkr` class also provides a way to wrap functions and automatically resolve their arguments.
Dkr supports synchronous and asynchronous functions.

---

## Dynamic Keyword Types

### **1. StringDk**
Handles placeholders in strings. Placeholders are defined using `{}`.
If the string only contains a single placeholder, the value itself will be emplaced directly. Otherwise the string will be formatted using the provided data.

Example:
```python
from deppy.helpers.wrappers.dkr import StringDk
dk = StringDk("{greeting}, {name}!")
result = dk.resolve({"greeting": "Hello", "name": "Alice"})
print(result)  # Output: "Hello, Alice!"
```

---

### **2. MappingDk**
Handles DK's in dictionaries.
Resolves its keys and values if they are DK's.

#### **Example:**
```python
from deppy.helpers.wrappers.dkr import MappingDk, StringDk
dk = MappingDk({StringDk("{key}"): StringDk("{value}")})
result = dk.resolve({"key": "key", "value": "val"})
print(result)  # Output: {'key': 'val'}
```

---

### **3. IterDk**
Handles DK's in iterables.

#### **Example:**
```python
from deppy.helpers.wrappers.dkr import IterDk, StringDk
dk = IterDk([StringDk("{first}"), StringDk("{second}")])
result = dk.resolve({"first": "one", "second": "two"})
print(result)  # Output: ['one', 'two']
```

---

### **4. JsonDk**
Automatically detects and resolves placeholders in complex JSON-like structures (combinations of strings, mappings, and iterables).

#### **Example:**
```python
from deppy.helpers.wrappers.dkr import JsonDk
dk = JsonDk({"data": ["{name}", {"key": "{value}"}]})
result = dk.resolve({"name": "Alice", "value": "val"})
print(result)  # Output: {'data': ['Alice', {'key': 'val'}]}
```

---

## Dynamic Keyword Resolver: `Dkr`

The `Dkr` class resolves multiple dynamic keyword arguments and can wrap functions to automatically handle these resolutions.

Example:
```python
from deppy.helpers.wrappers.dkr import Dkr, JsonDk

def post(url, json):
    return f"POST {url} with json {json}"

auth_request = Dkr(
    url="/authentication",
    json=JsonDk({"user": "{user}", "password": "{password}"}),
)(post, sub_name="auth") # This sub name will cause auth_request.__name__ to be "post_auth"


print(auth_request(user="Alice", password="1234"))  # Output: POST /authentication with json {'user': 'Alice', 'password': '1234'}
```

You can also use DKR as a decorator

```python
from deppy.helpers.wrappers.dkr import Dkr, JsonDk

@Dkr(url="/authentication", json=JsonDk({"user": "{user}", "password": "{password}"}))
def post(url, json):
    return f"POST {url} with json {json}"

print(post(user="Alice", password="1234"))  # Output: POST /authentication with json {'user': 'Alice', 'password': '1234'}
```

