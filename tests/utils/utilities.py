from unittest.mock import patch

def create_start_patcher(patch_function, patch_object=None, return_value=None, side_effect=None):
    if patch_object != None:
        patcher = patch.object(patch_object, patch_function)
    else:
        patcher = patch(patch_function)

    patcher_start = patcher.start()
    if return_value != None:
        patcher_start.return_value = return_value
    
    if side_effect != None:
        patcher_start.side_effect = side_effect
    
    return {'patcher' : patcher, 'patcher_start': patcher_start}

def stop_all_patchers(patchers):
    for patcher in patchers:
        patcher.stop()