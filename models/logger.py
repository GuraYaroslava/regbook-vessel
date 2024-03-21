import datetime

class Logger:
    def print_start_status(self, caption, level=1, current_datetime=None):
        current_datetime = current_datetime if current_datetime is not None else datetime.datetime.now()
        separator = '---' * level + '>'
        print('[{}]'.format(current_datetime), separator, caption)
        return

    def print_end_status(self, start_datetime, level=1, caption='Завершено'):
        end_datetime = datetime.datetime.now()
        duration = (end_datetime - start_datetime).total_seconds()
        separator = '---' * level + '>'
        if duration / 60 <= 1:
            print('[{}]'.format(datetime.datetime.now()), separator, caption, round(duration, 2), '(s)')
        else:
            duration = duration / 60.0
            print('[{}]'.format(datetime.datetime.now()), separator, caption, round(duration, 2), '(m)')

        return
