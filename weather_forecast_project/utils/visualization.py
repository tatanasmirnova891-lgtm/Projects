import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

def seconds_to_time(x, pos=None):
    hours = int(x // 3600)
    minutes = int((x % 3600) // 60)
    return f'{hours:02d}:{minutes:02d}'

def plot_weather(weather_info):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    sns.lineplot(data=weather_info, x='time_seconds', y='temp', hue='date', ax=ax1, marker='o')
    ax1.set_ylabel('Температура (°C)')
    ax1.set_title('Температура (°C)')
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(3600))
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(seconds_to_time))
    ax1.set_xlim(left=0)
    ax1.set_ylim(bottom=0)
    ax1.grid(True)

    sns.lineplot(data=weather_info, x='time_seconds', y='speed_wind_s', hue='date', ax=ax2, marker='s')
    ax2.set_ylabel('Скорость ветра (м/с)')
    ax2.set_xlabel('Время (часы:минуты)')
    ax2.set_title('Скорость ветра (м/с)')
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(3600))
    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(seconds_to_time))
    ax2.set_xlim(left=0)
    ax2.set_ylim(bottom=0)
    ax2.grid(True)

    tick_positions = [i * 3600 for i in range(0, 24, 3)]
    tick_labels = [f'{i:02d}:00' for i in range(0, 24, 3)]

    ax1.set_xticks(tick_positions)
    ax1.set_xticklabels(tick_labels)
    ax2.set_xticks(tick_positions)
    ax2.set_xticklabels(tick_labels)

    plt.tight_layout()
    plt.show()
