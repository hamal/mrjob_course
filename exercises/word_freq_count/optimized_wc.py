from mrjob.job import MRJob


class MRWordCountJob(MRJob):

    def steps(self):
        return [self.mr(
            mapper=self.capture_values,
            mapper_init=self.init_values,
            mapper_final=self.emit_values,
            reducer=self.sum_values
        )]
        
    # this is called before any input is consumed
    def init_values(self):
        self.characters = 0
        self.words = 0
        self.lines = 0

    # this is the mapper function you are familiar with, called once for each line of input
    def capture_values(self, _, line):
        self.characters += len(line)
        self.words += sum(1 for word in line.split() if word.strip())
        self.lines += 1

    # this is called after all input has been consumed, so we can emit the aggregate values
    def emit_values(self):
        yield 'characters', self.characters
        yield 'words', self.words
        yield 'lines', self.lines

    def sum_values(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordCountJob.run()
