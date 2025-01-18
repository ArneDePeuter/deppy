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
