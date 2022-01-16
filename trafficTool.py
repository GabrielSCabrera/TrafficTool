from datetime import datetime, timedelta, timezone
from mpl_toolkits.basemap import Basemap
import matplotlib.animation as animation
import matplotlib.pyplot as plt
from typing import List, Dict
import numpy as np
import requests
import json


class RoadTool:
    """
    For gathering traffic data using the Vegvesen API - API guide available at:
    https://www.vegvesen.no/trafikkdata/start/om-api
    """

    def __init__(self):
        pass

    @staticmethod
    def string_to_datetime(string: str):
        """
        Converts a string in the format "%Y-%m-%dT%H:%M:%S+02:00" to a datetime
        object.
        """
        return datetime.strptime(string[:-6], "%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def datetime_to_string(datetime_object: datetime):
        """
        Converts a datetime object to a string representing the date and time
        with localization GMT+02: "%Y-%m-%dT%H:%M:%S+02:00".
        """
        return datetime_object.strftime("%Y-%m-%dT%H:%M:%S+02:00")

    def request(self, query: str):
        """
        Makes a request to the Vegvesen API using a given query
        """
        url = "https://www.vegvesen.no/trafikkdata/api/"
        headers = {"content-type": "application/json"}
        data = query

        attempts = 10

        while attempts > 0:
            try:
                response = requests.post(url, headers=headers, data=data, timeout=5)
                break
            except requests.exceptions.ReadTimeout:
                attempts -= 1

        if attempts <= 0:
            msg = f"Error connecting to Vegvesen API - " f"Time Out"
            raise requests.exceptions.ReadTimeout(msg)

        if response.status_code != 200:
            msg = (
                f"Error connecting to Vegvesen API - "
                f"Status Code {response.status_code}"
            )
            raise ConnectionError(msg)
        return response

    def query_traffic_registration_point_search(
        self,
        roadCategoryIds: List[str] = None,
        countyNumbers: List[str] = None,
        isOperational: bool = None,
        trafficType: str = None,
        registrationFrequency: str = None,
    ):
        """
        Obtains a list of traffic registration points by ID, with optional
        filters.

        roadCategoryIds can take a list of the following strings (left column):

                "E"             European route
                "R"             National road
                "F"             County road
                "K"             Municipal road
                "P"             Private road

        countyNumbers can take a list of the following integers (left column):

                3               Oslo
                11              Rogaland
                15              Møre og Romsdal
                18              Nordland
                30              Viken
                34              Innlandet
                38              Vestfold og Telemark
                42              Agder
                46              Vestland
                50              Trøndelag
                54              Troms og Finnmark

        trafficType can take the string "VEHICLE" or "BICYCLE"
        registrationFrequency can take the string "CONTINUOUS" or "PERIODIC"
        """

        searchQuery = []
        if roadCategoryIds is not None:
            searchQuery.append(f"roadCategoryIds: [{', '.join(roadCategoryIds)}]")

        if countyNumbers is not None:
            searchQuery.append(
                f"countyNumbers: [{', '.join([str(i) for i in countyNumbers])}]"
            )

        if isOperational is not None:
            searchQuery.append(f"isOperational: {str(isOperational).lower()}")

        if trafficType is not None:
            searchQuery.append(f"trafficType: {str(trafficType).upper()}")

        if registrationFrequency is not None:
            searchQuery.append(
                f"registrationFrequency: {str(registrationFrequency).upper()}"
            )

        if searchQuery:
            searchQuery = ", ".join(searchQuery)
        else:
            searchQuery = ""

        query = (
            '{{"query":"{{trafficRegistrationPoints(searchQuery:{{{}}})'
            '{{id name location{{coordinates{{latLon{{lat lon}}}}}}}}}}"'
            ', "variables": null}}'
        )

        query = query.format(searchQuery)
        return query

    def query_traffic_volume_by_hour(
        self,
        trafficRegistrationPoints: List[str],
        start: datetime,
        stop: datetime,
    ):
        """
        Returns a list of hourly traffic volumes between two points in time
        """

        query_template = (
            '{{{{"query": "{{{{trafficData(trafficRegistrationPointId: \\"{}\\")'
            ' {{{{ volume {{{{ byHour(from: \\"{}\\", to: \\"{}\\") {{{{ edges '
            "{{{{ node {{{{from to total {{{{ volumeNumbers {{{{ volume }}}} "
            'coverage {{{{ percentage }}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}", '
            '"variables": null}}}}'
        )

        divisions = []
        t = start
        start_step = start
        t_count = timedelta()
        dt = timedelta(hours=1)
        query_hour_limit = timedelta(hours=99)
        while t < stop:
            if t_count >= query_hour_limit:
                t_count = timedelta()
                divisions.append((start_step, t))
                start_step = t
            t_count += dt
            t += dt
        if start_step != t:
            divisions.append((start_step, t))

        itermax = len(trafficRegistrationPoints) * len(divisions)
        perc = -1
        trafficVolumeByHour = {}

        print()

        for m, (start_step, stop_step) in enumerate(divisions):
            start_str = self.datetime_to_string(start_step)
            stop_str = self.datetime_to_string(stop_step)
            query_template_dated = query_template.format("{}", start_str, stop_str)

            for n, i in enumerate(trafficRegistrationPoints):
                query = query_template_dated.format(i)
                response = self.request(query)
                new_perc = round(100 * (n * m + n) / itermax, 1)
                if new_perc > perc:
                    perc = new_perc
                    print(f"\rDOWNLOADING - {perc}%", end = '')
                temp_list = []
                try:
                    for j in response.json()["data"]["trafficData"]["volume"]["byHour"][
                        "edges"
                    ]:
                        element = {
                            "start": self.string_to_datetime(j["node"]["from"]),
                            "stop": self.string_to_datetime(j["node"]["to"]),
                            "volume": j["node"]["total"]["volumeNumbers"]["volume"],
                            "coverage": j["node"]["total"]["coverage"]["percentage"],
                        }
                        temp_list.append(element)
                except TypeError:
                    pass
                    # print(f"No volume data for TRP {i}")
                else:
                    if i in trafficVolumeByHour.keys():
                        trafficVolumeByHour[i].extend(temp_list)
                    else:
                        trafficVolumeByHour[i] = temp_list

        print(f"DOWNLOADING - 100%")

        return trafficVolumeByHour

    def get_traffic_registration_points(
        self,
        roadCategoryIds: List[str] = None,
        countyNumbers: List[str] = None,
        isOperational: bool = None,
        trafficType: str = None,
        registrationFrequency: str = None,
    ):
        """
        Returns a list of datapoints containing the IDs, names, and coordinates
        of each traffic registration point located using the given search
        parameters
        """
        query = self.query_traffic_registration_point_search(
            roadCategoryIds,
            countyNumbers,
            isOperational,
            trafficType,
            registrationFrequency,
        )
        response = self.request(query)
        trafficRegistrationPoints = response.json()["data"]["trafficRegistrationPoints"]
        trafficRegistrationPoints = [
            {
                "id": i["id"],
                "name": i["name"],
                "lat": i["location"]["coordinates"]["latLon"]["lat"],
                "lon": i["location"]["coordinates"]["latLon"]["lon"],
            }
            for i in trafficRegistrationPoints
        ]
        return trafficRegistrationPoints

    def get_traffic_volume_by_hour(
        self, trafficRegistrationPoints: List[Dict], start: datetime, stop: datetime
    ):
        """
        Sorts the dictionary returned by method query_traffic_volume_by_hour
        by hour and includes coordinate data on each data point.
        """
        start = start.replace(microsecond=0, second=0, minute=0)
        stop = stop.replace(microsecond=0, second=0, minute=0)

        trafficVolumeByHour = road_tool.query_traffic_volume_by_hour(
            [i["id"] for i in trafficRegistrationPoints], start, stop
        )

        sortedTrafficVolumeByHour = {}
        t = start
        dt = timedelta(hours=1)
        while t < stop:
            sortedTrafficVolumeByHour[t] = {}
            t += dt

        max_volume = 0

        for i in trafficRegistrationPoints:
            id = i["id"]
            if id in trafficVolumeByHour.keys():
                for j in trafficVolumeByHour[id]:
                    sortedTrafficVolumeByHour[j["start"]][id] = {
                        "volume": j["volume"],
                        "lat": i["lat"],
                        "lon": i["lon"],
                    }
                    max_volume = max(max_volume, j["volume"])

        return sortedTrafficVolumeByHour, max_volume

    def plot_map(self, water_color: str = "lightblue", land_color: str = "lightgreen"):
        """
        Plots a map of Norway using pyplot and basemap
        """
        plt.style.use("dark_background")
        fig, ax = plt.subplots()

        fig.set_dpi(100)
        fig.set_size_inches(8, 11)

        m = Basemap(
            llcrnrlon=-1.0,
            urcrnrlon=40.0,
            llcrnrlat=55.0,
            urcrnrlat=75.0,
            resolution="i",
            projection="lcc",
            lat_1=65.0,
            lon_0=5.0,
        )
        m.drawmapboundary(fill_color=water_color)
        m.drawcountries()
        parallels = np.arange(0.0, 81, 2.0)
        m.drawparallels(parallels, labels=[False, True, True, False], textcolor="w")
        meridians = np.arange(-10, 351.0, 5.0)
        m.drawmeridians(meridians, labels=[True, False, False, True], textcolor="w")
        m.fillcontinents(color=land_color, lake_color=water_color)
        m.drawcoastlines()

        return m

    def plot_map_points(
        self,
        lat: List[float],
        lon: List[float],
        markers: List[str] = None,
        colors: List[str] = None,
        sizes: List[int] = None,
        water_color: str = "lightblue",
        land_color: str = "lightgreen",
    ):
        """
        Plots a set of given coordinates as points onto a map of Norway.  If
        given, markers/colors/sizes must be of equal length to x and y.
        """
        m = self.plot_map(water_color, land_color)
        for i in range(len(lat)):
            x_i = lon[i]
            y_i = lat[i]
            marker = "."
            color = "r"
            size = "5"
            if markers is not None:
                marker = markers[i]
            if colors is not None:
                color = colors[i]
            if sizes is not None:
                size = sizes[i]
            x_i, y_i = m(x_i, y_i)
            m.plot(x_i, y_i, 3, marker=marker, color=color, ms=size)

    def plot_traffic_registration_points(
        self,
        roadCategoryIds: List[str] = None,
        countyNumbers: List[str] = None,
        isOperational: bool = None,
        trafficType: str = None,
        registrationFrequency: str = None,
    ):
        """
        Given a set of filters, plots the traffic registration points.
        """
        trafficRegistrationPoints = self.get_traffic_registration_points(
            roadCategoryIds,
            countyNumbers,
            isOperational,
            trafficType,
            registrationFrequency,
        )
        lat = []
        lon = []
        for i in trafficRegistrationPoints:
            lat.append(i["lat"])
            lon.append(i["lon"])
        self.plot_map_points(lat, lon)

    def plot_traffic_volume(
        self,
        datetime_object: datetime,
        sortedTrafficVolume: Dict,
        max_volume: int,
        water_color: str = "lightblue",
        land_color: str = "lightgreen",
    ):
        """
        Plots the traffic volume of a set of traffic registration points on a
        map of Norway
        """
        lat = []
        lon = []
        markers = []
        colors = []
        sizes = []
        max_size = 40
        for i in sortedTrafficVolume.values():
            lat.append(i["lat"])
            lon.append(i["lon"])
            markers.append(".")
            colors.append("r")
            scale = int(max_size * float(i["volume"]) / max_volume)
            sizes.append(scale)
        m = self.plot_map_points(
            lat, lon, markers, colors, sizes, water_color, land_color
        )
        plt.xlabel(f"Date & Time {datetime_object}", labelpad=30, fontweight="bold")
        return m

    def animate_traffic_volume(
        self,
        sortedTrafficVolumeByHour: Dict,
        max_volume: int,
        water_color: str = "lightblue",
        land_color: str = "lightgreen",
        save_as: str = None,
    ):
        m = self.plot_map(water_color, land_color)

        points = []

        lat = []
        lon = []
        markers = []
        colors = []
        sizes = []
        max_size = 40
        min_size = 5
        datetimes = sorted(sortedTrafficVolumeByHour.keys())

        for i in sortedTrafficVolumeByHour[datetimes[0]].values():
            lat.append(i["lat"])
            lon.append(i["lon"])
            markers.append(".")
            colors.append("r")
            scale = (max_size * float(i["volume"]) / max_volume) + min_size
            sizes.append(scale)

        for n in range(len(lon)):
            x_n, y_n = m(lon[n], lat[n])
            point = m.plot(
                x_n, y_n, 3, marker=markers[n], color=colors[n], ms=sizes[n]
            )[0]
            points.append(point)

        xlabel = plt.xlabel(
            f"Date & Time {datetimes[0]}", labelpad=30, fontweight="bold"
        )

        def init():

            lat = []
            lon = []
            markers = []
            colors = []
            sizes = []

            for i in sortedTrafficVolumeByHour[datetimes[0]].values():
                lat.append(i["lat"])
                lon.append(i["lon"])
                markers.append(".")
                colors.append("r")
                scale = (max_size * float(i["volume"]) / max_volume) + min_size
                sizes.append(scale)

            for n, point in enumerate(points):
                x_n, y_n = m(lon[n], lat[n])
                point.set_data(x_n, y_n)
                point.set_marker(markers[n])
                point.set_color(colors[n])

            plt.gca().set_xlabel(
                f"Date & Time {datetimes[0]}", labelpad=30, fontweight="bold"
            )

            return points

        def animate(i):

            lat = []
            lon = []
            markers = []
            colors = []
            sizes = []

            for j in sortedTrafficVolumeByHour[datetimes[i]].values():
                lat.append(j["lat"])
                lon.append(j["lon"])
                markers.append(".")
                colors.append("r")
                scale = (max_size * float(j["volume"]) / max_volume) + min_size
                sizes.append(scale)

            for n, point in enumerate(points):
                try:
                    x_n, y_n = m(lon[n], lat[n])
                    point.set_data(x_n, y_n)
                    point.set_marker(markers[n])
                    point.set_color(colors[n])
                    point.set_markersize(sizes[n])
                except:
                    point.set_markersize(0)

            plt.gca().set_xlabel(
                f"Date & Time {datetimes[i]}", labelpad=30, fontweight="bold"
            )
            return points

        anim = animation.FuncAnimation(
            plt.gcf(),
            animate,
            init_func=init,
            frames=len(datetimes),
            interval=200,
            blit=False,
        )

        if save_as is None:
            print("Displaying Animation")
            plt.show()
            plt.close()
        else:
            print("Saving Animation")
            anim.save(f"{save_as}.mp4", writer="ffmpeg", fps=16)
            plt.close()

    def traffic_animation(
        self,
        start: datetime,
        stop: datetime,
        roadCategoryIds: List[str] = None,
        countyNumbers: List[str] = None,
        isOperational: bool = None,
        trafficType: str = None,
        registrationFrequency: str = None,
        save_as: str = None,
    ):
        """
        Creates an animation of the traffic volume for the selected timeframe
        and given filters.
        """
        trp = road_tool.get_traffic_registration_points(
            roadCategoryIds,
            countyNumbers,
            isOperational,
            trafficType,
            registrationFrequency,
        )
        tvbh, max_volume = road_tool.get_traffic_volume_by_hour(trp, start, stop)
        road_tool.animate_traffic_volume(
            sortedTrafficVolumeByHour=tvbh, max_volume=max_volume, save_as=save_as
        )


if __name__ == "__main__":
    road_tool = RoadTool()
    start = datetime(year=2019, month=10, day=24, hour=0)
    stop = datetime(year=2019, month=10, day=31, hour=0)
    save_as = "animation"
    road_tool.traffic_animation(
        start, stop, trafficType="Vehicle", save_as=save_as
    )
