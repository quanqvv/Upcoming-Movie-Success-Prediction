from tkinter import *
from tkinter.font import Font
from tkinter.ttk import Style

import pandas

import pathmng
from preprocessing import data_model as datamodel
from preprocessing.data_model import DataModel

fields = ["Title", 'MPAA Rating', 'Runtime', 'Studio', 'Release Date', 'Budget', 'Award', 'Genre',
          'Plot Des', "Real success result"]
value_default = ["", "WHITE MISCHIEF", "PG-13", "108", "Aurora Productions LLC", "1987-1-5", "3075658", "0",
                 "Comedy|Romance",
                 "A Southern California coed parties with her roommates and flirts with a surfer next door.", "", ""]
value_default = [""] * len(value_default)

real_field = ["title", "mpaa_rating", "runtime", "studio", "theater_release_date", "budget", "award",
              "genre",
              "plot_des", "success"]

success_cell = None

def get_real_field_name(name_field):
   return real_field[fields.index(name_field)]


def parse_data(entries):
    clip_board_data = root.clipboard_get()
    # raw_data = entries["Raw Data"].get().replace("\n", "").split("\t")
    raw_data = clip_board_data.replace("\n", "").split("\t")
    order_field = ["title", "plot_des", "release_year", "runtime", "budget", "box_office_gross", "studio",
                   "mpaa_rating",
                   "theater_release_date", "directors", "casts ", "genre", "award", "is_holiday", "success"]
    field_to_data = {}
    for index, data in enumerate(raw_data):
        if index == len(order_field):
            break
        field = order_field[index]
        field_to_data[field] = data

    for index, field in enumerate(fields):
        real_field = get_real_field_name(field)
        if real_field in field_to_data:
            entry = entries[field]
            entry: Entry
            entry.delete(0, "end")
            entry.insert(0, field_to_data[real_field])


def check_success(entries, result_var):
    movie_data = {}
    for index, field in enumerate(fields):
        movie_data[real_field[index]] = entries[field].get()

    import pickle
    data_model = datamodel.get_data_model()
    data_model: DataModel

    model_dict = pickle.load(open(pathmng.model_list_path, "rb"))
    results = []
    for model, name in model_dict.items():
        result = model.predict([data_model.get_input_feature(movie_data)])
        result = "True" if result[0] == 1 else "False"
        # more = "".join([" "] * (40 - len(name)))
        # results.append(f"{name}:{more}{result}")
        results.append([name, result])
    print([_.__len__() for _ in results])
    x = pandas.DataFrame(results, columns=["Model Name", "Result"]).to_string(col_space=10)
    print(x)
    result_var.set(x)


def makeform(root, fields):
    entries = {}
    for index, field in enumerate(fields):
        row = Frame(root)
        lab = Label(row, width=22, text=field + ": ", anchor='w')
        ent = Entry(row, width=60)
        ent.insert(0, value_default[index])
        row.pack(side=TOP, fill=X, padx=5, pady=5)
        lab.pack(side=LEFT)
        ent.pack(side=RIGHT, expand=YES, fill=X)
        entries[field] = ent

    result_var = StringVar()
    fontStyle = Font(family="Lucida Grande", size=10, weight="normal")
    row = Frame(root)
    lab = Label(row, width=22, text="Predicted" + ": ", anchor='w')
    ent = Message(row, width=300, textvariable=result_var, relief=RAISED, font=fontStyle)
    row.pack(side=TOP, fill=X, padx=5, pady=5)
    lab.pack(side=LEFT)
    ent.pack(side=RIGHT, expand=YES, fill=X)

    # result_var.set()

    return entries, result_var


if __name__ == '__main__':
    # parse_data()

    root = Tk()
    root.title("Demo Upcoming Movie Success Prediction")
    style = Style(root)
    style.theme_use("vista")

    ents, result_var = makeform(root, fields)
    root.bind('<Return>', (lambda event, e: None))
    b3 = Button(root, text='Parse',
                command=(lambda e=ents: parse_data(e)))
    b3.pack(side=LEFT, padx=5, pady=5)
    b2 = Button(root, text='Check Success',
                command=(lambda e=ents, p=result_var: check_success(e, result_var)))
    b2.pack(side=LEFT, padx=5, pady=5)

    root.mainloop()
