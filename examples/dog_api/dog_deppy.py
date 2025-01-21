from api import DogApi
from deppy.blueprint import Blueprint, Node, Object, Output


class DogDeppy(Blueprint):
    api = Object(DogApi)

    get_breeds_request = Node(api.get_breeds)
    breeds = Output(
        get_breeds_request,
        extractor=lambda data: [
            (breed, sub_breeds) for breed, sub_breeds in data["message"].items()
        ],
    )
    breed_info = Output(breeds, loop=True)
    breed = Output(breed_info, extractor=lambda data: data[0])
    sub_breeds = Output(breed_info, extractor=lambda data: data[1])
    sub_breed = Output(sub_breeds, loop=True)

    breed_images_request = Node(api.get_breed_images, inputs=[breed])
    breed_images = Output(breed_images_request, extractor=lambda data: data["message"])
    sub_breed_images_request = Node(api.get_sub_breed_images, inputs=[breed, sub_breed])
    sub_breed_images = Output(
        sub_breed_images_request, extractor=lambda data: data["message"]
    )
