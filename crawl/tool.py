from concurrent.futures import ThreadPoolExecutor

import pandas

import pathmng
import utils


class MovieMetadata:
    def __str__(self):
        return str(vars(self))

    def get_all_attr(self):
        res = []
        for attr, _ in self.__dict__.items():
            res.append(attr)
        return res

    def get_all_value(self):
        values = []
        for attr, value in self.__dict__.items():
            values.append(value)
        return values

    def get_all_normalized_values(self):
        values = []
        for attr, value in self.__dict__.items():
            if type(value) is set:
                values.append(list(value))
            else:
                values.append(value)
        return values

    def get_all_props(self):
        return self.__dict__.items()


def save_metadata_to_csv(metadatas: [MovieMetadata], path: str):
    if len(metadatas) == 0:
        print("Can not save empty dataframe")
        return
    import pandas
    base_obj = metadatas[0]
    metadatas = [_.get_all_normalized_values() for _ in metadatas]
    movie_metadata_df = pandas.DataFrame(data=metadatas, columns=base_obj.get_all_attr())

    print(metadatas)

    # detect previous df
    pre_columns = []
    try:
        pre_df = pandas.read_csv(path)
        pre_columns = list(pre_df.columns)
    except:
        pass

    if pre_columns == list(movie_metadata_df.columns):
        movie_metadata_df.to_csv(path, index=False, header=False, mode="a")
        print("Save dataframe by APPEND mode")
    else:
        movie_metadata_df.to_csv(path, index=False)
        print("Save dataframe by NEW mode")


class StreamingWriter:
    pass


def streaming_crawl(source_list: list, func, path_save_file, number_per_group=100, start_group=0):
    import time
    for index, group in enumerate(utils.split_list_to_group(number_per_group, _list=source_list)):
        if index < start_group:
            continue
        print("GROUP:", index)
        movie_metadatas = []
        with ThreadPoolExecutor(max_workers=50) as e:
            for arg in group:
                e.submit(lambda arg: movie_metadatas.append(func(arg)), arg)

        save_metadata_to_csv(list(filter((None).__ne__, movie_metadatas)), path_save_file)
        time.sleep(5)

def append_metadata_to_csv():
    pass


if __name__ == '__main__':
    x = MovieMetadata()
    x.name = ["a'"]
    x.id = "\"'ok,"
    save_metadata_to_csv([x], pathmng.temp_path)

    print(pandas.read_csv(pathmng.temp_path))