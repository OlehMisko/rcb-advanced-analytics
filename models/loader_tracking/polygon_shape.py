from typing import List, Tuple
from shapely.geometry import Polygon, Point


# TODO: Convert northing and easting coordinates

class PolygonShape:
    def __init__(
            self,
            polygon_points: List[Tuple[float, float]]):
        self.update_polygon(polygon_points)

    def point_inside_shape(self, point: Tuple[float, float]) -> bool:
        """
        Detects whether a given point is inside the polygon.
        Converts the tuple of points into a shapely Point.
        :param point: tuple of values that represent point in space
        :return: whether the point is inside the polygon
        """
        return self.polygon.contains(Point(point))

    def update_polygon(self, polygon_points: List[Tuple[float, float]]):
        """
        Updates the current state of the polygon
        :param polygon_points: list of tuples that represent
        points in space
        """
        self.polygon_points = polygon_points
        self.polygon = Polygon(self.polygon_points)
