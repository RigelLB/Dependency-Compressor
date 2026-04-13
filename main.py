from typing                     import  Tuple, List
from collections                import  defaultdict, deque
from packaging.requirements     import  Requirement, InvalidRequirement
from packaging.version          import  Version
from concurrent.futures         import  ThreadPoolExecutor, as_completed
import                                  hashlib
import                                  json
import                                  os
import                                  subprocess
import                                  argparse

# Constants
HOME_DIR            = os.getcwd()
DEPENDENCY_PATH     = f"{HOME_DIR}/input/pypi_bandersnatch_requirements.txt"
OUTPUT_PATH         = f"{HOME_DIR}/output/final.txt"
VERSIONS_PATH       = f"{HOME_DIR}/output/all_package_versions.txt"
SAVED_STATE_PATH    = f"{HOME_DIR}/input/pypi_saved_state.json"
MAX_WORKERS         = 16

# Default values
Node                                        = Tuple[str, str]   # (package, version)
dep_map:            dict[Node, set[None]]   = defaultdict(set)  # Forward dependency graph
reverse_map:        dict[Node, set[Node]]   = defaultdict(set)  # Reverse dependency graph (who depends on the package)
processed:          set[Node]               = set()             # Processed nodes
ref_count:          dict[Node, int]         = defaultdict(int)  # Reference count (how many packages depend on a node)
latest_version_map: dict[str, str]          = {}                # Latest version lookup


def get_input_dependencies() -> List[str]:
    """
    Reads from the defined DEPENDENCY_PATH constant and returns a list of packages from it

    Returns:
        List[str]: List of packages defined from the dependency path constant (DEPENDENCY_PATH)
    """
    packages = []
    
    try:
        with open(DEPENDENCY_PATH) as file:
            packages = file.readlines()
    except Exception as e:
        print("Could not read from input dependency file. Did you set the dependency path?")
        return None
    
    return packages

def get_all_packages_versions(packages: List[str]) -> set:
    """
    Takes a list of standard defined packages (ex: librosa>=0.9.0) and find all versions for all packages that meats the requirement.

    Args:
        packages (List[str]): List of packages
    
    Returns:
        set[Node]: Returns a set of `Node`'s corresponding to the package and versions
    """
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

    Args:
        package (str): python requirement package (ex: librosa==0.9.0)
    
    Returns: 
        List[Node]: List of `Node`'s containing all versions corresponding to the requirement
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
            if "versions:" not in resp.stdout:
                raise ValueError("Unexpected pip output format")
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

def get_latest_version(pkg: str) -> Node:
    """
    Returns the newest version node available for a given package.

    Args:
        pkg (str): python requirement package (ex: librosa==0.9.0)
    
    Returns:
        Node: Latest version node of the given package
    """
    try:
        package_name    = Requirement(pkg).name
        cmd             = ["pip", "index", "versions", package_name]
        resp            = subprocess.run(cmd, capture_output=True, text=True)
        if "versions:" not in resp.stdout:
            raise ValueError("Unexpected pip output format")

        resp            = resp.stdout.split("versions:")[1]
        
        # Make sure that installed packages don't mess with version parsing
        try:
            resp = resp.split("\nINSTALLED")[0]
        except Exception as e:
            pass

        # Filter out only the newest version
        latest_version  = resp.replace(" ", "").split(",")[0]

        update_latest(pkg, latest_version)

        return (pkg, latest_version)
    
    except Exception as e:
        print(f"Error fetching out the latest version. Error: {e!r}")
        return (pkg, None)


def resolve_node(pkg: str, version_spec : str | None = None) -> Node: 
    """
    Resolves packages to valid versions. Used to ensure package with versions
    defined as LATEST have a valid version.

    Args:
        pkg (str): The package name
        version_spec (str | None): the package version
    Returns:
        Node: Returns a resolved node
    """
    # Make sure to always resolve for the newest version
    if version_spec is None or version_spec == "LATEST": 
        _, version              = get_latest_version(pkg)
        latest_version_map[pkg] = version
        version_spec            = version

    return (pkg, version_spec) 


def add_package(pkg: str, version: str | None = None): 
    """
    Adds a given package and version to the dependency graph.

    Args:
        pkg (str): The package name
        version (str | None): the package version
    """
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
    Takes in a package in the form package[extras]>=version and all the available versions 
    and adds it the the dependency graph.

    Args:
        pkg (str): Package defined as package[extras]>(separator)=version (ex: librosa==0.9.0)
    """
    # Need to find all versions for it
    all_versions = get_all_versions_from_package(pkg)

    # Call add_package for all versions
    for pkg, version in all_versions:
        add_package(pkg, version)

def fetch_dependencies(pkg, version) -> List[Tuple[str, str]]:
    """
    Fetches all the dependency for a version defined package. 

    Args:
        pkg (str): The package name
        version (str | None): the package version
    
    Returns:
        List[Tuple[str, str]]: Dependencies defined as lists of (package, version) tuples
    """
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
    """
    Attempts to remove a node from the graph. 
    If the package is a dependent of another package the package will not be removed.

    Args:
        Node: Package that is version defined  
    """
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
    """
    Attempts to update the version map for the packages defined as LATEST. 
    If the new version is not the same as the previous version it will resolve for new dependencies.

    Args:
        pkg (str): The package name
        version (str | None): the package version
    """
    old_version             = latest_version_map.get(pkg)
    latest_version_map[pkg] = new_version

    # Add new version, make sure the the graph contains the newest version
    if new_version != old_version:
        build_graph([(pkg, new_version)])

        # Remove old version if unused
        if old_version:
            old_node = (pkg, old_version)
            try_remove_node(old_node)


def get_roots() -> set[Node]:
    """
    Returns only the packages that are not a dependency to other packages.

    Returns:
        set[Node]: The root nodes
    """
    all_nodes = set(dep_map.keys())
    dependent_nodes = set(reverse_map.keys())
    return all_nodes - dependent_nodes

def build_graph(initial_nodes):
    """
    Takes in a iterable of nodes and builds the dependency graph while also resolving dependencies.

    Args:
        - Iter[Node]: Iterable of `Node` objects (ex: List[Node] or set[Node]). 
    """


    def process_node(node) -> Tuple[Node, List[Tuple[str, str]]]:
        """
        Resolves a node and fetches all dependencies for a given node.

        Args:
            node (Node): Package that is version defined  
        
        Returns:
            Tuple[Node, List[Tuple[str, str]]]: A tuple of the original node and a list of dependencies for the original node.
        """
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

def split_pkg(pkg: str) -> Tuple[str, set]:
    """
    Attempts to get the extra packages included in the package.
    Ex: typer[all]==1.0.0 will return (typer[all]==1.0.0, all)

    Args:
        pkg (str): A version defined package (ex: librosa==0.9.0)

    Returns:
        Tuple[str, set]: Returns the original package and any extras if present
    """
    if "[" in pkg:
        base, rest = pkg.split("[", 1)
        extras = rest.rstrip("]").split(",")
        return base, set(extras)
    return pkg, set()

def flatten_graph_merge_extras(dep_map: dict[Node, set[Node]]) -> set[Node]:
    """
    Returns all nodes from the graph while only keeping nodes that are unique (typer and typer[all] will only return typer[all])

    Args:
        dep_map (dict[Node, set[Node]]): Dependency map for the different packages
    
    Returns:
        set[Node]: All the nodes from the graph (package, version)
    """
    all_nodes = set(dep_map.keys())

    for deps in dep_map.values():
        all_nodes.update(deps)

    # Make sure that all the nodes a resolved (no LATEST as the versions)
    resolved_nodes = set()
    for pkg, version in all_nodes:
        resolved_nodes.add(resolve_node(pkg, version))

    grouped = {}

    for pkg, version in resolved_nodes:
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
    """
    Computes the hash of the input packages. 
    Meant to be used with the packages defined from the dependency file.

    Args:
        lines (list[str]): List of strings (normally list of package names and version requirements)
    """
    normalized = "".join(line.strip() + "\n" for line in lines)
    return hashlib.sha256(normalized.encode()).hexdigest()

def save_state(path: str, input_lines: List[str]):
    """
    Saves the dependency graph and input that was used to build the current graph to a json file.

    Args:
        path (str): The path to which to save the state file
        input_lines (list[str]): List of strings (normally list of package names and version requirements)
    """
    data = {
        "dep_map": {
            encode_node(node): [encode_node(dep) for dep in deps]
            for node, deps in dep_map.items()
        },
        "latest_version_map":   latest_version_map,
        "input_lines":          [line.strip() for line in input_lines],
        "input_hash":           compute_input_hash(input_lines),
    }

    with open(path, "w") as f:
        json.dump(data, f, indent="    ")

def load_state(path: str):
    """
    Loads the state file from the given path into the global dependency graph and relevant maps

    Args:
        path: The path to which to open the state file
    """
    global dep_map, reverse_map, ref_count, processed, latest_version_map

    with open(path) as f:
        data = json.load(f)

    # Reset everything
    reset_all()

    # Rebuild dep_map
    loaded_dep_map = {}

    for node_str, deps_list in data.get("dep_map", {}).items():
        node = decode_node(node_str)

        deps = set()
        for dep_str in deps_list:
            deps.add(decode_node(dep_str))

        loaded_dep_map[node] = deps

    dep_map.update(loaded_dep_map)

    # Restore latest version map
    latest_version_map.update(data.get("latest_version_map", {}))

    # Rebuild derived maps
    for node, deps in dep_map.items():
        processed.add(node)

        for dep in deps:
            reverse_map[dep].add(node)
            ref_count[dep] += 1

    # Return the lines and hash to compare the diff later
    return {
        "input_lines": data.get("input_lines", []),
        "input_hash": data.get("input_hash", None),
    }
    
def diff_inputs(old_lines: List[str], new_lines: List[str]) -> Tuple[set, set]:
    """
    Check for differences between the two different lists of lines.
    Returns the added and removed lines as a Tuple.

    Args:
        old_lines (List[str]): List of strings
        new_lines (List[str]): List of strings

    Returns:
        Tuple[set, set]: Returns the added and removed lines as a Tuple.
    """
    old_set = set(line.strip() for line in old_lines)
    new_set = set(line.strip() for line in new_lines)

    added   = new_set - old_set
    removed = old_set - new_set

    return added, removed

def handle_input_change(old_lines: List[str], new_lines: List[str]) -> Tuple[str, set | None]:
    """
    Returns the appropriate action to take depending on the difference between the two inputs
    Ex: 
    -   If no lines are changed -> noop (do nothing) 
    -   If a line is added      -> add (add the new packages)
    -   If a line is removed    -> rebuild (rebuild the graph from the input)

    Args:
        old_lines (List[str]): List of strings from the previous input
        new_lines (List[str]): List of strings from the new input

    Returns:    
        Tuple[str, set | None]: Returns the action and the possible set of packages to add
    """
    added, removed = diff_inputs(old_lines, new_lines)

    if not added and not removed:
        print("No changes — skipping build")
        return "noop", None

    if removed:
        print("Detected removal or modification — rebuilding graph")
        return "rebuild", None

    if added:
        print(f"New packages detected: {added}")
        return "add", added
    

def reset_all():
    """
    Resets the dependency map and the related maps.
    """

    dep_map.clear()
    reverse_map.clear()
    ref_count.clear()
    processed.clear()
    latest_version_map.clear()

def encode_node(node: Node) -> str:
    """
    Returns the string representation of a node

    Args:
        node (Node): Package that is version defined 
    
    Returns:
        str: Returns the string representation of a node
    """
    pkg, version = node
    return f"{pkg}=={version}"

def decode_node(s: str) -> Node:
    """
    Decodes a string containing the following structure (package==version) into a `Node` object

    Returns:
        Node: Package that is version defined
    """
    pkg, version = s.split("==", 1)
    return (pkg, version)

def get_latest_in_graph(pkg: str) -> str | None:
    """
    Returns the latest version contained in the graph

    Args:
        pkg (str): python requirement package (ex: librosa==0.9.0)

    Returns:
        str: The newest version contained in the graph
    """
    package_name = Requirement(pkg).name

    versions = []

    for node in dep_map:
        node_pkg, node_version = node
        if node_version == "LATEST":
            continue

        if node_pkg == package_name:
            versions.append(node_version)

    if not versions:
        return None

    return str(max(Version(v) for v in versions))

def check_for_new_pypi_versions(package: str) -> List[Node]:
    """
    Checks to see if the graph has to most recent version of the package.

    Args:
        package (str): python requirement package (ex: librosa==0.9.0)
    
    Returns: 
        List[Node]: List of `Node`'s containing all versions that the graph does not contain
    """
    new_nodes               = []
    pacakge_name            = Requirement(package).name

    graph_latest_version    = get_latest_in_graph(package)

    if graph_latest_version is None:
        print(f"The graph does not contain any versions of the package {package}, {graph_latest_version}")
        return []

    # Compare it to the versions from pypi
    cmd                     = ["pip", "index", "versions", pacakge_name]
    resp                    = subprocess.run(cmd, capture_output=True, text=True)
    if "versions:" not in resp.stdout:
        raise ValueError("Unexpected pip output format")
    available_versions      = resp.stdout.split("versions:")[1].replace(" ", "").split(",")

    # If the current latest version in our graph is not at the beginning of the list that means newer versions are available
    version_index           = available_versions.index(graph_latest_version)

    if version_index != 0:
        for version in available_versions[:version_index]:
            new_nodes.append((package, version.strip()))

    return new_nodes

if __name__ == "__main__":   
    # Initial variables
    state_file_exists       = False
    full_reset              = False
    rebuild                 = False

    # Parse the input arguments
    parser                  = argparse.ArgumentParser()
    parser.add_argument("--reset", help="Force a full reset of the graph (do not load state file)", action="store_true")
    args                    = parser.parse_args()

    if args.reset:
        full_reset = True
    
    packages                = get_input_dependencies()
    if not packages:
        exit()

    # Check to see if saved state file exsists
    state_file_exists = os.path.exists(SAVED_STATE_PATH)
    check_versions    = False

    if state_file_exists and not full_reset:
        try:
            state = load_state(SAVED_STATE_PATH)

            # The state did not contain the dependency map
            if dep_map == {}:
                raise

            if state["input_hash"] == compute_input_hash(packages):
                print("No changes determined from input file.")
                check_versions = True

            action = handle_input_change(state["input_lines"], packages)

            if action[0] == "noop":
                check_versions = True

            elif action[0] == "rebuild":
                reset_all()
                full_package_node_list = get_all_packages_versions(packages)
                build_graph(full_package_node_list)

            elif action[0] == "add":
                check_versions  = True
                new_packages    = action[1]

                for pkg in new_packages:
                    nodes = get_all_versions_from_package(pkg)

                    build_graph(nodes)
            
            if check_versions:
                # Check input packages to see if there are any available new packages
                print("Checking for new available pypi packages")

                # Multithread the pip checking        
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {}
                    
                    for package in packages:
                        print(f"Package checking in pypi: {package.strip()}")
                        
                        if (">=" in package) or (">=" not in package and "==" not in package):
                            futures[executor.submit(check_for_new_pypi_versions, package.strip())] = package
                    
                    for future in as_completed(futures):
                        try:
                            nodes = future.result()
                            if nodes:
                                build_graph(nodes)
                        except Exception as e:
                            package = futures[future]
                            print(f"Error checking {package}: {e!r}")
                

        except Exception as e:
            print(f"Error: {e!r}. Could not load state file properly. Rebuilding graph.")
            rebuild = True

    if rebuild:
        print("Rebuilding graph")
        # No saved state
        reset_all()
        full_package_node_list = get_all_packages_versions(packages)
        
        # For sanity write to versions to packages
        with open(VERSIONS_PATH, "w") as file:
            for pkg in sorted([(pkg[0]+"=="+pkg[1]) for pkg in full_package_node_list], key=str.lower):
                file.write(pkg + "\n")

        build_graph(full_package_node_list)

    save_state(SAVED_STATE_PATH, packages)

    # Write to the final output file to requirements
    with open(OUTPUT_PATH, "w") as file:
        for pkg in sorted(flatten_graph_merge_extras(dep_map=dep_map), key=str.lower):
            file.write(pkg + "\n")
