from datetime import datetime

import logging

logger = logging.getLogger(__name__)

class Refresher:
    """
    A class that represents a refresher for processing collections. Helps keeping the ratings in the collections
    up to date by refreshing items that are newly added or newly premiered.

    Attributes:
        emby (object): An instance of the Emby class.
        seconds_between_requests (int): The number of seconds between each request.
        processed_items (list): A list of already processed item IDs so they don't get processed again.
    """

    def __init__(self, emby):
        self.emby = emby
        self.seconds_between_requests = 1
        self.processed_items = []

    def process_collection(
        self,
        collection_id: int,
        max_days_since_added: int,
        max_days_since_premiered: int,
        show_rating_change: bool = False,
    ):
        """
        Process a collection based on specified criteria.
        Both max_days_since_added and max_days_since_premiered must be satisfied for an item to be refreshed.

        Args:
            collection_id (int): The ID of the emby collection to process.
            max_days_since_added (int): Will be refreshed if the item was added to Emby less than this number of days ago.
            max_days_since_premiered (int): Will be refreshed if the item premiered less than this number of days ago.
            show_rating_change (bool): If True, will logger.info the rating change for each item, requires an additional API request for each item.
        """

        items_in_collection = self.emby.get_items_in_collection(
            collection_id, ["PremiereDate", "DateCreated", "CommunityRating"]
        )

        current_date = datetime.now()
        for item in items_in_collection:
            # Example item: {'Id': '1541497', 'PremiereDate': '2023-11-08T09:27:58.0000000Z', 'DateCreated': '2023-12-08T09:27:58.0000000Z'}
            # Check if DateCreated is less than 7 days and PremiereDate is less than 30 days
            # Current date

            if item["Id"] in self.processed_items:
                # logger.info(f"Item already processed: {item['Id']} {item['Name']}")
                continue

            self.processed_items.append(item["Id"])

            created_date = None
            try:
                created_date = datetime.fromisoformat(
                    item["DateCreated"].replace("Z", "+00:00")
                )
                created_date = created_date.replace(tzinfo=None)
            except Exception as e:
                logger.info(
                    f"Error parsing DateCreated ({item['DateCreated']}) for {item['Id']}: {item['Name']}. Error: {e}"
                )
                continue

            premier_date = None

            if item["PremiereDate"] is None:
                logger.info(
                    f"Premiere date missing. Setting date to now: {item['Id']} {item['Name']}"
                )
                premier_date = current_date
            else:
                premier_date = datetime.fromisoformat(
                    item["PremiereDate"].replace("Z", "+00:00")
                )
                premier_date = premier_date.replace(tzinfo=None)

            days_since_created = (current_date - created_date).days
            days_since_premiered = (current_date - premier_date).days

            if days_since_premiered > max_days_since_premiered:
                # logger.info(f"Premiered {max_days_since_premiered} days ago: {item['Name']}")
                continue

            if days_since_created > max_days_since_added:
                # logger.info(f"Added more than {max_days_since_added} days ago {item['Name']}")
                continue

            r = self.emby.refresh_item(item["Id"])
            if r is True:
                logger.info(f"    {item['Name']}")
                if not show_rating_change:
                    continue
                old_rating = item["CommunityRating"]
                item = self.emby.get_item(item["Id"])  # Get new rating
                if "CommunityRating" not in item:
                    item["CommunityRating"] = 0
                new_rating = item["CommunityRating"]
                logger.info(f"    Rating change {old_rating} -> {new_rating}")
            else:
                logger.error(f"ERROR: Item refresh fail: {item['Id']} {item['Name']}")


def main():
    pass


if __name__ == "__main__":
    main()
