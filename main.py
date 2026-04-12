from typing                     import  Tuple, List
from collections                import  defaultdict, deque
from packaging.requirements     import  Requirement, InvalidRequirement
from concurrent.futures         import  ThreadPoolExecutor, as_completed
import                                  hashlib
import                                  json
import                                  os
import                                  subprocess

# Constants
HOME_DIR            = os.getcwd()
DEPENDENCY_PATH     = f"{HOME_DIR}/input/pypi_bandersnatch_requirements.txt"
OUTPUT_PATH         = f"{HOME_DIR}/output/final.txt"
SAVED_STATE_PATH    = f"{HOME_DIR}/input/pypi_saved_state.json"
MAX_WORKERS         = 16

# Default values
Node                                        = Tuple[str, str]   # (package, version)
dep_map:            dict[Node, set[None]]   = defaultdict(set)  # Forward dependency graph
reverse_map:        dict[Node, set[Node]]   = defaultdict(set)  # Reverse dependency graph (who depends on the package)
processed:          set[Node]               = set()             # Processed nodes
ref_count:          dict[Node, int]         = defaultdict(int)  # Reference count (how many packages depend on a node)
latest_version_map: dict[str, str]          = {}                 # Latest version lookup


def get_input_dependencies() -> List[str]:
    """
    Reads from the defined DEPENDENCY_PATH constant and returns a list of packages from it
    """
    packages = []
    
    try:
        with open(DEPENDENCY_PATH) as file:
            packages = file.readlines()
    except Exception as e:
        print("Could not read from input dependency file. Did you set the dependency path?")
        return None
    
    return packages

def get_all_packages_versions(packages) -> set:
    full_package_list = set()

     # Fetch all packages and versions
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_all_versions_from_package, package.strip()) for package in packages]

        for future in as_completed(futures):
            full_package_list.update(future.result())
    
    return full_package_list

def get_all_versions_from_package(package: str) -> List[Node]:
    """
    Takes in a package string formatted as the following (ex: librosa>=0.9.0) and 
    returns all the versions possible for the package as a set.
    """
    package_name            = ""
    package_version         = ""
    get_all_versions        = True
    all_package_versions    = []

    print(f"Getting all versions for '{package}'")

    # Extract package name and extras
    try:
        package_obj     = Requirement(package)
        package_name    = package_obj.name
    except InvalidRequirement:
        print(f"Could not parse a valid requirement from the package. Skipping {package}")
        return []
    
    # Get package versions
    # Get only the specific package
    if   "==" in package:
        get_all_versions    = False
        split_package       = package.split("==")
        package_version     = split_package[1].strip()

    # Make sure to get all versions
    elif ">=" in package:
        split_package       = package.split(">=")
        package_version     = split_package[1].strip()

    else:
        get_all_versions    = False
        all_package_versions.append((package_name, "LATEST"))

    if get_all_versions:
        try:
            cmd             = ["pip", "index", "versions", package_name]
            resp            = subprocess.run(cmd, capture_output=True, text=True)
            resp            = resp.stdout.split("versions:")[1]
            
            # Make sure that installed packages don't mess with version parsing
            try:
                resp = resp.split("\nINSTALLED")[0]
            except Exception as e:
                pass

            # Only keeps packages including and newer
            split_versions  = resp.replace(" ", "").split(",")
            version_index   = split_versions.index(package_version)
            split_versions  = split_versions[:version_index + 1]
            extras          = f"[{','.join(package_obj.extras)}]" if package_obj.extras else ""
            
            all_package_versions.extend((f"{package_name}{extras}", version) for version in split_versions)
        
        except Exception as e:
            print(f"Could not fetch version for package. Error: {e!r}")
            print(f"Including only latest ({package})")
            # TODO: Add to failed list
            all_package_versions.append((package_name, "LATEST"))
    
    return all_package_versions

def get_latest_version(pkg):
    try:
        package_name    = Requirement(pkg).name
        cmd             = ["pip", "index", "versions", package_name]
        resp            = subprocess.run(cmd, capture_output=True, text=True)
        resp            = resp.stdout.split("versions:")[1]
        
        # Make sure that installed packages don't mess with version parsing
        try:
            resp = resp.split("\nINSTALLED")[0]
        except Exception as e:
            pass

        # Filter out only the newest version
        latest_version  = resp.replace(" ", "").split(",")[0]

        return (pkg, latest_version)
    
    except Exception as e:
        print(f"Error fetching out the latest version. Error: {e!r}")
        return (pkg, None)


def resolve_node(pkg, version_spec : str | None = None) -> Node: 
    if version_spec is None or version_spec == "LATEST": 
        version = latest_version_map.get(pkg)
        if version == None:
            _, version              = get_latest_version(pkg)
            latest_version_map[pkg] = version
            version_spec            = version

    return (pkg, version_spec) 


def add_package(pkg: str, version: str | None = None): 
    start_node  = resolve_node(pkg, version) 
    queue       = deque([start_node]) 
     
    while queue: 
        pkg, version = queue.popleft() 
        if (pkg, version) in processed: 
            continue 

        deps                    = fetch_dependencies(pkg, version) 
        dep_map[(pkg, version)] = deps 
        processed.add((pkg, version)) 

        for dep_pkg, dep_spec in deps: 
            dep_node = resolve_node(dep_pkg, dep_spec) 
            
            # Track reverse edges 
            reverse_map.setdefault(dep_node, set()).add((pkg, version)) 
            
            # Track references 
            ref_count[dep_node] = ref_count.get(dep_node, 0) + 1 
            if dep_node not in processed: 
                queue.append(dep_node)

def add_package_all_versions(pkg: str):
    """
    Takes in a package in the form package[extras]>=version
    """
    # Need to find all versions for it
    all_versions = get_all_versions_from_package(pkg)

    # Call add_package for all versions
    for pkg, version in all_versions:
        add_package(pkg, version)

def fetch_dependencies(pkg, version):
    print(f"Getting all dependencies for '{pkg}=={version}'")
    # Pipe the input and output from pip-compile to avoid writing to files
    cmd = ["pip-compile","-", "--no-header", "--no-annotate","--strip-extras", "--output-file=-"]

    resp = subprocess.run(cmd, capture_output=True, text=True, input=f"{pkg}=={version}")
    resp = resp.stdout

    deep_dependencies = resp.removeprefix("\n").removesuffix("\n")
    deep_dependencies = deep_dependencies.split("\n")
    deep_dependencies = [tuple(pkg.split("==")) for pkg in deep_dependencies]
    deep_dependencies = [dep for dep in deep_dependencies if len(dep) == 2]  # Filter out malformed

    return deep_dependencies


def try_remove_node(node: Node):
    if ref_count.get(node, 0) > 0:
        return  # still in use

    # Remove dependencies
    for dep in dep_map.get(node, []):
        reverse_map[dep].discard(node)
        ref_count[dep] -= 1

        # recursively clean up
        try_remove_node(dep)

    dep_map.pop(node, None)
    processed.discard(node)


def update_latest(pkg: str, new_version: str):
    old_version = latest_version_map.get(pkg)

    latest_version_map[pkg] = new_version

    # Add new version
    add_package(pkg, new_version)

    # Remove old version if unused
    if old_version:
        old_node = (pkg, old_version)
        try_remove_node(old_node)


def get_roots():
    all_nodes = set(dep_map.keys())
    dependent_nodes = set(reverse_map.keys())
    return all_nodes - dependent_nodes

def build_graph(initial_nodes):

    def process_node(node):
        resolved_node = resolve_node(node[0], node[1])
        pkg, version = resolved_node
        return resolved_node, fetch_dependencies(pkg, version)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}

        for node in initial_nodes:
            if node not in processed:
                processed.add(node)
                futures[executor.submit(process_node, node)] = node

        while futures:

            for future in as_completed(list(futures)):
                node = futures.pop(future)

                try:
                    _, deps = future.result()
                except Exception:
                    continue  # optionally log error

                dep_map[node] = set()

                for dep_pkg, dep_version in deps:

                    dep_node = resolve_node(dep_pkg, dep_version)

                    # ----------------------------
                    # Single-threaded graph mutation
                    # ----------------------------

                    dep_map[node].add(dep_node)
                    reverse_map[dep_node].add(node)
                    ref_count[dep_node] += 1

                    # ----------------------------
                    # Queue new work safely
                    # ----------------------------

                    if dep_node not in processed:
                        processed.add(dep_node)
                        futures[executor.submit(process_node, dep_node)] = dep_node

def split_pkg(pkg: str):
    if "[" in pkg:
        base, rest = pkg.split("[", 1)
        extras = rest.rstrip("]").split(",")
        return base, set(extras)
    return pkg, set()

def flatten_graph_merge_extras(dep_map):
    all_nodes = set(dep_map.keys())

    for deps in dep_map.values():
        all_nodes.update(deps)

    # Make sure that all the nodes a resolved (no LATEST as the versions)
    resolved_nodes = set()
    for pkg, version in all_nodes:
        resolved_nodes.add(resolve_node(pkg, version))

    grouped = {}

    for pkg, version in all_nodes:
        base, extras = split_pkg(pkg)
        key = (base, version)

        if key not in grouped:
            grouped[key] = set(extras)
        else:
            grouped[key].update(extras)

    result = set()

    for (base, version), extras in grouped.items():
        if extras:
            extras_str = ",".join(sorted(extras))
            result.add(f"{base}[{extras_str}]=={version}")
        else:
            result.add(f"{base}=={version}")

    return result

def compute_input_hash(lines: list[str]) -> str:
    normalized = "".join(line.strip() + "\n" for line in lines)
    return hashlib.sha256(normalized.encode()).hexdigest()

def save_state(path, input_lines):
    data = {
        "dep_map": {
            encode_node(node): [encode_node(dep) for dep in deps]
            for node, deps in dep_map.items()
        },
        "latest_version_map": latest_version_map,
        "input_lines": [line.strip() for line in input_lines],
        "input_hash": compute_input_hash(input_lines),
    }

    with open(path, "w") as f:
        json.dump(data, f, indent="    ")

def load_state(path):
    global dep_map, reverse_map, ref_count, processed, latest_version_map

    import json

    with open(path) as f:
        data = json.load(f)

    # --- Reset everything FIRST ---
    reset_all()

    # --- Rebuild dep_map safely ---
    loaded_dep_map = {}

    for node_str, deps_list in data.get("dep_map", {}).items():
        node = decode_node(node_str)

        # Important: ensure uniqueness + correct type
        deps = set()
        for dep_str in deps_list:
            deps.add(decode_node(dep_str))

        loaded_dep_map[node] = deps

    dep_map.update(loaded_dep_map)

    # --- Restore latest version cache ---
    latest_version_map.update(data.get("latest_version_map", {}))

    # --- Rebuild derived structures ---
    for node, deps in dep_map.items():
        processed.add(node)

        for dep in deps:
            reverse_map[dep].add(node)
            ref_count[dep] += 1

    # --- Return input snapshot (for your diff logic) ---
    return {
        "input_lines": data.get("input_lines", []),
        "input_hash": data.get("input_hash", None),
    }
    
def diff_inputs(old_lines, new_lines):
    old_set = set(line.strip() for line in old_lines)
    new_set = set(line.strip() for line in new_lines)

    added   = new_set - old_set
    removed = old_set - new_set

    return added, removed

def handle_input_change(old_lines, new_lines):
    added, removed = diff_inputs(old_lines, new_lines)

    if not added and not removed:
        print("No changes — skipping build")
        return "noop"

    if removed:
        print("Detected removal or modification — rebuilding graph")
        return "rebuild"

    if added:
        print(f"New packages detected: {added}")
        return "add", added
    

def reset_all():
    dep_map.clear()
    reverse_map.clear()
    ref_count.clear()
    processed.clear()
    latest_version_map.clear()

def encode_node(node: Node) -> str:
    """
    Returns the string representation of a node
    """
    pkg, version = node
    return f"{pkg}=={version}"

def decode_node(s) -> Node:
    pkg, version = s.split("==", 1)
    return (pkg, version)

if __name__ == "__main__":
    # When someone adds a package with you have to resolve all the versions initially
    # If do_reset do a full search from pypi_bander...
    state_file_exists       = False
    packages                = get_input_dependencies()
    if not packages:
        exit()

    # Check to see if saved state file exsists
    state_file_exists = os.path.exists(SAVED_STATE_PATH)

    if state_file_exists:
        try:
            state = load_state(SAVED_STATE_PATH)

            if state["input_hash"] == compute_input_hash(packages):
                print("No changes, exit")
                exit()

            action = handle_input_change(state["input_lines"], packages)

            if action == "noop":
                exit()

            elif action == "rebuild":
                reset_all()
                full_package_node_list = get_all_packages_versions(packages)
                build_graph(full_package_node_list)

            elif action[0] == "add":
                new_packages = action[1]

                for pkg in new_packages:
                    nodes = get_all_versions_from_package(pkg)
                    
                    build_graph(nodes)

        except Exception as e:
            print("Could not load state file properly. Rebuilding graph.")
            state_file_exists = False

    else:
        # No saved state
        reset_all()
        full_package_node_list = get_all_packages_versions(packages)
        
        # For sanity write to versions to packages
        with open("versions.txt", "a") as file:
            for pkg in sorted([(pkg[0]+"=="+pkg[1]) for pkg in full_package_node_list], key=str.lower):
                file.write(pkg + "\n")

        build_graph(full_package_node_list)

    save_state(SAVED_STATE_PATH, packages)

    # Write to the final output file to requirements
    with open(OUTPUT_PATH, "w") as file:
        for pkg in sorted(flatten_graph_merge_extras(dep_map=dep_map), key=str.lower):
            file.write(pkg + "\n")
