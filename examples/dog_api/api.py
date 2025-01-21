from deppy.helpers.asyncclient import AsyncClient
from deppy.helpers.wrappers.dkr import Dkr, StringDk


class DogApi(AsyncClient):
    def __init__(self, base_url: str = "https://dog.ceo/api/") -> None:
        super().__init__(base_url=base_url)
        self.dkr = Dkr()

        self.get_breeds = Dkr(url="breeds/list/all")(self.get, "breeds")

        self.get_breed_images = Dkr(url=StringDk("breed/{breed}/images"))(
            self.get, "breed_images"
        )

        self.get_sub_breed_images = Dkr(
            url=StringDk("breed/{breed}/{sub_breed}/images")
        )(self.get, "sub_breed_images")
