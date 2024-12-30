import os

def def_word_cnt(string):
    list_string = string.split(' ')
    dict = {}
    for i in range(len(list_string)):
        count = 1
        for j in range(i + 1, len(list_string)):
            if list_string[i] == list_string[j]:
                count += 1
        if list_string[i] not in dict.keys():
            dict[list_string[i]] = count
    return dict

string = input("Nhập: ")
dict = def_word_cnt(string)
print(dict)


# def create_json_file(file_number):
#     file_name = f'result_{file_number}.json'
#     file_path = 'folder_json_file'
#     file = file_path + "/" + file_name
#     with open(file, 'w') as f:
#         f.write(f'File số{file_number}')


# list(map(lambda x: create_json_file(x), range(1, 101)))