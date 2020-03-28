from typing import Union, Iterable, List, Callable
from enum import Enum
import numpy as np

from ..config import CONST
from ..logger import empty_logger


class InterpolateDirection(Enum):
    LEFT = 'left'
    RIGHT = 'right'


def lin_interp_from_first_max(
        array: np.ndarray,
        threshold: float,
        direction: InterpolateDirection
) -> Union[float, int, None]:
    """
    Returns the linear interpolated index of a decimal `threshold` of the first
    maximum in the 1-dimensional `array` in `direction` from the index of that
    first maximum.
    """

    index_peak = array.argmax()
    target_value = array[index_peak] * threshold

    if threshold == 1:
        return index_peak

    elif direction == InterpolateDirection.RIGHT:
        if index_peak == array.size:
            return index_peak
        candidates = np.where(array[index_peak:] <= target_value)[0]
        if candidates.size <= 0:
            return None
        index_found = candidates[0] + index_peak
        value_found = array[index_found]
        if value_found == target_value:
            return index_found
        index_prev = index_found - 1
        value_prev = array[index_prev]
        index_gap = (value_prev - target_value) / (
                value_prev - value_found)
        return index_prev + index_gap

    elif direction == InterpolateDirection.LEFT:
        if index_peak == 0:
            return index_peak
        candidates = np.where(array[:index_peak] <= target_value)[0]
        if candidates.size <= 0:
            return None
        index_found = candidates[-1]
        value_found = array[index_found]
        if value_found == target_value:
            return index_found
        index_prev = index_found + 1
        value_prev = array[index_prev]
        index_gap = (value_prev - target_value) / (
                value_prev - value_found)
        return index_prev - index_gap

    else:
        raise ValueError("invalid `direction`")


def calc_scaled_waveform(
        waveform: np.ndarray,
        lin_factor: np.ndarray,
        pow2_factor: np.ndarray,
        logger=empty_logger()
) -> np.ndarray:
    """
    Return ASIRAS waveform echoes scaled to remove the effects of gains
    and attenuations.

    Scaled echoes result should be in Watts according to field 26,
    pg 56 of data products description.
    """

    logger.info("scaling waveform")

    return (
            10e-9
            * (2 ** pow2_factor)
            * lin_factor
            * waveform.swapaxes(0, 1)
    ).swapaxes(0, 1)


def calc_first_bin_elvtn(
        bin_size: float,
        rwc_delay: np.ndarray,
        sensor_elvtn: np.ndarray,
        num_bins: int,
        logger=empty_logger()
) -> np.ndarray:
    """
    Returns an `np.ndarray` of the first range bin elevation for each ASIRAS
    row based on the numpy arrays `rwc_delay` and `sensor_elvtn`
    """
    logger.info("calculating first range bin elevation")

    # range window is the physical distance covered by all the bins
    rwc_size = bin_size * num_bins

    # distance from sensor to range window center (m)
    # distance from start of first bin to center (halfway) of the range window
    rwc_dist = (rwc_delay * 0.5 * CONST.c)

    # delay / 2 (due to two-way travel time) times the speed of light
    # (speed_light)
    rwc_half = rwc_size / 2  # rwc_offset in old MATLAB code

    # distance from sensor to first bin (m)
    first_bin_dist = rwc_dist - rwc_half

    # elevation of first bin start
    return sensor_elvtn - first_bin_dist


def calc_tfmra_elevation(
        thresholds: List[float],
        waveform: np.ndarray,
        first_bin_elvtn: np.ndarray,
        bin_size: float,
        logger=empty_logger()
) -> np.ndarray:
    """
    Returns a 2d array where each cell is the estimated ice surface
    elevation. Rows are  ASIRAS observations (row of `waveform`) and columns
    are the thresholds (item of `threshold`)
    """

    elevations = np.empty((waveform.shape[0], len(thresholds)))

    for i, t in enumerate(thresholds):
        logger.info(f"threshold {t} {i + 1}/{len(thresholds)}")

        f = lambda array: lin_interp_from_first_max(
            array, t, InterpolateDirection.LEFT
        )

        elevations[:, i] = first_bin_elvtn - (
                np.apply_along_axis(f, 1, waveform) * bin_size
        )

    return elevations


def aggregate_relative_indices(
        array: np.ndarray,
        indices: Iterable[int],
        starting_index_func: Callable,
        aggregate_func: Callable
):
    """
    Applies `aggregate_func` to the values of `indices` of `array` that are
    relative to the index returned when `starting_index_func` is applied to
    `array`.
    """
    starting_index = starting_index_func(array)
    get_indices = [
        starting_index + i
        for i in indices
        if 0 <= starting_index + i < len(array)
    ]
    return aggregate_func(array[get_indices])
