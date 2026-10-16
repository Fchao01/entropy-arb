"""Venue-specific rules for reduce-only taker orders, separate from openings."""


def reduction_rules(venue, common_step, configured_min_quote):
    step = 10.0 ** -venue.size_decimals if hasattr(venue, 'size_decimals') else common_step
    return (step, getattr(venue, 'reduce_only_min_base', venue.min_base),
            getattr(venue, 'reduce_only_min_quote', max(configured_min_quote, venue.min_quote)))
