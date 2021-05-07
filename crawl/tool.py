
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
    import pandas
    print(metadatas)
    base_obj = metadatas[0]
    metadatas = [_.get_all_normalized_values() for _ in metadatas]
    movie_metadata_df = pandas.DataFrame(data=metadatas, columns=base_obj.get_all_attr())
    movie_metadata_df.to_csv(path, index=False)
    print(movie_metadata_df)


if __name__ == '__main__':
    print(type(dict()) is set)