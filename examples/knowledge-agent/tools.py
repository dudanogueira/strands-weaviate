"""Plain function tools for the example agent. These do not use Weaviate."""

from strands import tool

# Grams of water per gram of coffee, matching the handbook's brewing guide.
WATER_PER_GRAM_OF_COFFEE = {
    "pour-over": 250 / 15,
    "french press": 500 / 30,
    "cold brew": 1000 / 100,
}


@tool
def brew_ratio(method: str, water_grams: float) -> str:
    """Calculate how much ground coffee to use for an amount of water.

    Use this whenever the customer asks how much coffee they need for a specific amount of
    water, instead of doing the arithmetic yourself.

    Args:
        method: The brewing method: "pour-over", "french press" or "cold brew".
        water_grams: The amount of water in grams. One millilitre of water weighs one gram,
            so one litre is 1000.
    """
    normalized = method.strip().lower().replace("_", " ").replace("pour over", "pour-over")
    ratio = WATER_PER_GRAM_OF_COFFEE.get(normalized)
    if ratio is None:
        return f"Unknown method '{method}'. Choose one of: {', '.join(WATER_PER_GRAM_OF_COFFEE)}."
    if water_grams <= 0:
        return "The amount of water must be greater than zero."
    coffee_grams = water_grams / ratio
    return (
        f"Use {coffee_grams:.0f} grams of coffee for {water_grams:.0f} grams of water "
        f"({normalized}, 1 to {ratio:.1f})."
    )
