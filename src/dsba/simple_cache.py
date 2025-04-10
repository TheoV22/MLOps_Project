import os
import pickle
import functools

def cache_to_disk(cache_file, use_cache=True):
    """
    A decorator to cache the result of a function to a file on the disk
    
    Parameters:
    cache_file (str): The file path to store the cached result
    use_cache (bool): Whether to attempt to load the cached result
    
    Returns:
    Decorated function that loads from or writes to the cache file
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # If caching is enabled and the cache file exists, load it
            if use_cache and os.path.exists(cache_file):
                try:
                    with open(cache_file, "rb") as f:
                        print(f"[Cache] Loading result from {cache_file}")
                        result = pickle.load(f)
                    return result
                except Exception as e:
                    print(f"[Cache] Failed to load cache: {e}. Recomputing...")
            # Compute the result if cache is not available
            print(f"[Cache] Computing result and caching to {cache_file}")
            result = func(*args, **kwargs)
            try:
                os.makedirs(os.path.dirname(cache_file), exist_ok=True)
                with open(cache_file, "wb") as f:
                    pickle.dump(result, f)
                print(f"[Cache] Result cached to {cache_file}")
            except Exception as e:
                print(f"[Cache] Failed to write cache: {e}")
            return result
        return wrapper
    return decorator
