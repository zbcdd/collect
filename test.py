import pickle


def main():
    PATH = '/data1/TraceMDRCA/data/raw_data/fault/2022-12-06T03:50:01.276865022+08:00_2022-12-06T03:55:14.893743971+08:00.pkl'
    with open(PATH, 'rb') as f:
        data = pickle.load(f)
    


if __name__ == '__main__':
    main()
