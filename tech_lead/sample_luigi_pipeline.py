import luigi
from pathlib import Path
import pandas as pd


class PathParameter(luigi.Parameter):
    """
    Luigi parameter whose value is a ``pathlib.Path``.

    This is a small helper to make working with Path parameters easier in Luigi,
    no need to do anything with this.
    """
    def parse(self, x):
        return Path(x)

    def serialize(self, x):
        return str(x)


class PulseRateFile(luigi.ExternalTask):
    """
    Raw input file for the sensor readings from the HR sensor for a single test.

    In Luigi all input files are represented as external tasks, without
    a `run()` method.
    """
    test_directory = PathParameter()

    def output(self):
        return luigi.LocalTarget(str(self.test_directory / 'hr' / 'hr.csv'))


class SingleTestProcessingTask(luigi.Task):
    """
    Task representing the processing tasks performed on the data of a single tester.

    In this toy pipeline this is implemented as a simple mean calculation of all HR values
    for each stimuli (video shown to the testers).
    """
    test_directory = PathParameter()

    def requires(self):
        return PulseRateFile(test_directory=self.test_directory)

    def output(self):
        return luigi.LocalTarget(str(self.test_directory / 'hr' / 'hr_processed.csv'))

    def run(self):
        test_id = Path(self.test_directory).name
        df = pd.read_csv(self.input().path)

        result = (df.groupby('stimulus_number')
                    .filter(lambda rows: len(rows) > 10)
                    .groupby('stimulus_number').mean())
        result.rename(columns={'IBT': "IBT_per_stimuli_{}".format(test_id)}, inplace=True)

        result.to_csv(self.output().path)


class AllTestsProcessingTask(luigi.Task):
    """
    Task representing the aggregation step, collecting the processed results for all testers
    and calculating the mean IBT value for each stimuli.

    Calls `SingleTestProcessingTask` for each test directory and saves the result in a single
    merged file. This is the final output of this pipeline.
    """
    project_directory = PathParameter()

    def requires(self):
        requires = [SingleTestProcessingTask(test_directory=test_directory)
                    for test_directory in self.project_directory.iterdir()
                    if test_directory.is_dir()]
        return requires

    def output(self):
        return luigi.LocalTarget(str(self.project_directory / 'hr_merged.csv'))

    def run(self):
        input_dfs = []

        for requirement in self.input():
            single_test_df = pd.read_csv(requirement.path, index_col=0)
            input_dfs.append(single_test_df)

        merged_df = pd.concat(input_dfs, axis=1)
        merged_df['IBT_per_stimuli_mean'] = merged_df.mean(axis=1)
        merged_df.to_csv(self.output().path)


if __name__ == '__main__':
    luigi.run(['--project-directory', 'data'],
              main_task_cls=AllTestsProcessingTask)
