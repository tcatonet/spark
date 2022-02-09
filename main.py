from csv_analyse import Csv_analyse
import time

if __name__ == '__main__':
    csv_analyse = Csv_analyse()

    csv_analyse.display_commit_24_mounth()
    csv_analyse.display_10_first_word_in_commit()
    csv_analyse.display_projet_with_max_commit()
    csv_analyse.display_best_contributor()

    time.sleep(100000)