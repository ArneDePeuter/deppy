
# 🐶 Example: Scraping the Dog API for All Images

This example demonstrates how to scrape the [Dog API](https://dog.ceo/dog-api/) for **all images of dogs**. We'll walk through the process step by step. 🐾

---

## 📌 1. Accessing the Dog API

```python
from deppy.helpers.asyncclient import AsyncClient
from deppy.helpers.wrappers.dkr import Dkr, StringDk


class DogApi(AsyncClient):
    def __init__(self, base_url: str = "https://dog.ceo/api/") -> None:
        super().__init__(base_url=base_url)
        self.dkr = Dkr()

        self.get_breeds = Dkr(
            url="breeds/list/all"
        )(self.get, "breeds")

        self.get_breed_images = Dkr(
            url=StringDk("breed/{breed}/images")
        )(self.get, "breed_images")

        self.get_sub_breed_images = Dkr(
            url=StringDk("breed/{breed}/{sub_breed}/images")
        )(self.get, "sub_breed_images")
```

The `DogApi` class inherits from `AsyncClient` and provides the following methods:  
- 🐾 **Get Breeds**: Fetches a list of all breeds.  
- 📷 **Get Breed Images**: Retrieves images for a specific breed.  
- 📸 **Get Sub-Breed Images**: Retrieves images for a specific sub-breed.  

These methods correspond 1-1 to the API documentation so this step should straightforward. ✅

---

## 📌 2. Defining Our Deppy

```python
from api import DogApi
from deppy.blueprint import Blueprint, Node, Object, Output


class DogDeppy(Blueprint):
    api = Object(DogApi)

    get_breeds_request = Node(api.get_breeds)
    breeds = Output(get_breeds_request, extractor=lambda data: [(breed, sub_breeds) for breed, sub_breeds in data["message"].items()])
    breed_info = Output(breeds, loop=True)
    breed = Output(breed_info, extractor=lambda data: data[0])
    sub_breeds = Output(breed_info, extractor=lambda data: data[1])
    sub_breed = Output(sub_breeds, loop=True)

    breed_images_request = Node(api.get_breed_images, inputs=[breed])
    breed_images = Output(breed_images_request, extractor=lambda data: data["message"])
    sub_breed_images_request = Node(api.get_sub_breed_images, inputs=[breed, sub_breed])
    sub_breed_images = Output(sub_breed_images_request, extractor=lambda data: data["message"])
```

### Explanation 📝
- **Step 1**: Fetch the list of breeds and transform the data into tuples (breed, sub-breeds).  
- **Step 2**: Create unique scopes for each breed using a loop.  
- **Step 3**: Extract relevant data (e.g., breeds, sub-breeds).  
- **Step 4**: Fetch images for breeds and sub-breeds.  

### Visual Representation 🌟
The result flow looks like this:  
![Result Flow](images/img.png)

---

## 📌 3. Running the Deppy and Querying the Results

```python
from dog_deppy import DogDeppy
import asyncio


def flatten_list(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]


async def main():
    deppy = DogDeppy()
    # deppy.dot("dog_deppy.dot")
    result = await deppy.execute()

    breed_images = result.query(deppy.breed_images)
    sub_breed_images = result.query(deppy.sub_breed_images)

    all_dog_images = flatten_list(breed_images) + flatten_list(sub_breed_images)
    print(all_dog_images)


if __name__ == "__main__":
    asyncio.run(main())
```

### Steps 🚀
1. Initialize your `DogDeppy`.  
2. Execute the blueprint and query results.  
3. Combine images from breeds and sub-breeds.  
4. Flatten the list to get all images in one place.  

And voilà! 🎉 You've scraped all the dog images from the API! 🐕  
![Scraped Images](images/img_1.png)

### 🛠️ Let Deppy Do the Heavy Lifting
The beauty of using Deppy lies in its simplicity. All we had to do was:

1. Design our functions to interact with the Dog API.
2. Link nodes in a logical flow to handle data processing.

Deppy takes care of the rest:
- It ensures optimal execution, automatically managing asynchronous calls and dependencies.
- It handles all the complex orchestration of nodes, making sure everything runs efficiently.

By abstracting away the hard stuff, Deppy allows us to focus on what matters—building clear and maintainable logic! 🚀
