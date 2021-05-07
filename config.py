import os
import file
driver_path = os.path.abspath(os.path.dirname(file.__file__)+r"\chromedriver.exe")
browser_profile_path = os.path.abspath(os.path.dirname(file.__file__)+r"\profile")
crawl_data_path = os.path.abspath(os.path.dirname(file.__file__)+r"\crawl")
saving_data_path = os.path.abspath(os.path.dirname(file.__file__)+r"\saving")


def auto_create_path(*paths):
    for path in paths:
        os.makedirs(os.path.dirname(path), exist_ok=True)


if __name__ == '__main__':
    print(driver_path)