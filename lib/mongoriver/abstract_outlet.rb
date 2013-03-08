module Mongoriver
  class AbstractOutlet

    # implement these methods in your subclass
    def update_optime(timestamp); end

    def insert(db_name, collection_name, document); end
    def remove(db_name, collection_name, document); end
    def update(db_name, collection_name, selector, update); end

    def create_index(db_name, collection_name, index_key, options); end
    def drop_index(db_name, collection_name, index_name); end

    def create_collection(db_name, collection_name); end
    def drop_collection(db_name, collection_name); end
    def rename_collection(db_name, old_collection_name, new_collection_name); end

    def drop_database(db_name); end
  end
end