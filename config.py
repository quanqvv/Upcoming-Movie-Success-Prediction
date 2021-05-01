import os
import file
driver_path = os.path.abspath(os.path.dirname(file.__file__)+r"\chromedriver.exe")
browser_profile_path = os.path.abspath(os.path.dirname(file.__file__)+r"\profile")
crawl_data_path = os.path.abspath(os.path.dirname(file.__file__)+r"\crawl")


if __name__ == '__main__':
    print(driver_path)