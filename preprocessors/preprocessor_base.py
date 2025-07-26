from abc import abstractmethod, ABC

class PreprocessorBase(ABC):

    @abstractmethod
    def process_df(self, df):
        pass