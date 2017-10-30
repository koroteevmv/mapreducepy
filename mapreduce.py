class MapReduce:
    """ Mapreduce jobs simulation
    
    This class simulates very simle, python-native implementation of MapReduce
    programming paradigm. You can use it with your own map, reduce and reader
    functions.
    
    
    Usage:
        data = [
            [1, "2"],
            [1, "3"]
        ]
        
        def array_reader():
            for row in data:
                yield row
        def word_count_map(key, value):
            res = []
            for word in value.split():
                word = word.lower()
                res.append((word, 1))
            return res
        def word_count_reduce(key, value):
            return [(key, sum(value))]
            
        x = MapReduce(mapper=word_count_map, 
                      reducer=word_count_reduce, 
                      reader=array_reader)
        word_count = x.execute(printing=True)

    
    """
    def __init__(self, mapper=None, reducer=None, reader=None):
        self.queue = []
        self.mapper = mapper
        self.reducer = reducer
        self.reader = reader

    def _iter_group(self, queue):
        buf = []
        res = []
        prev_key = None
        for val in queue:
            cur_key, cur_val = val
            if cur_key == prev_key or prev_key is None:
                buf.append(cur_val)
            else:
                res.append((prev_key, buf))
                buf = []
                buf.append(cur_val)
            prev_key = cur_key
        if buf:
            res.append((cur_key, buf))
        return res

    def execute(self, printing=False):
        # MAP step
        for k, v in self.reader():
            self.queue.extend(self.mapper(k, v))
        if printing:
            print("After MAP step:")
            for k, v in self.queue:
                print(k, v)
            print()

        # SHUFFLE step
        self.queue = sorted(self.queue, key=lambda a: a[0])
        self.queue = self._iter_group(self.queue)
        if printing:
            print("After SHUFFLE step:")
            for k, v in self.queue:
                print(k, v)
            print()

        # REDUCE step
        res = []
        for k, v in self.queue:
            res.extend(self.reducer(k, v))
        if printing:
            print("After REDUCE step:")
            for k, v in res:
                print(k, v)
            print()
            print()
        return res

# TODO: сделать сортировку более грамотно

# TODO: реализовать пользователские комбайнеры

# TODO: добавить шаблоны применения


data = [
    [1, "2"],
    [1, "3"]
]

def array_reader():
    for row in data:
        yield row

def word_count_map(key, value):
    res = []
    for word in value.split():
        word = word.lower()
        res.append((word, 1))
    return res
    
def word_count_reduce(key, value):
    return [(key, sum(value))]


x = MapReduce(mapper=word_count_map, 
              reducer=word_count_reduce, 
              reader=array_reader)

word_count = x.execute(printing=True)