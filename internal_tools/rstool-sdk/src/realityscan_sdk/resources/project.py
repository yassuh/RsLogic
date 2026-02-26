
from typing import Any, Dict, List, Optional, Sequence

from ..models.project import RSProjectStatus
from ..models.tasks import TaskHandle, TaskStatus
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import RealityScanClient


class ProjectAPI:
    def __init__(self, client: "RealityScanClient") -> None:
        self._c = client

    # ---- session/project lifecycle ----

    def create(self) -> str:
        """
        GET /project/create
        Returns Session header; client.session is updated and returned.
        """
        self._c._request("GET", "/project/create", require_session=False)
        if not self._c.session:
            raise RuntimeError("Expected Session header from /project/create, but none was returned.")
        return self._c.session

    def open(self, guid: str, *, name: Optional[str] = None) -> str:
        """
        GET /project/open?guid=...&name=...
        Returns Session header; client.session is updated and returned.
        """
        params: Dict[str, Any] = {"guid": guid}
        if name:
            params["name"] = name
        self._c._request("GET", "/project/open", require_session=False, params=params)
        if not self._c.session:
            raise RuntimeError("Expected Session header from /project/open, but none was returned.")
        return self._c.session

    def close(self) -> None:
        """GET /project/close"""
        self._c._request("GET", "/project/close", require_session=True)

    def disconnect(self) -> None:
        """GET /project/disconnect"""
        self._c._request("GET", "/project/disconnect", require_session=True)

    def delete(self, guid: str) -> None:
        """GET /project/delete?guid=..."""
        self._c._request("GET", "/project/delete", require_session=False, params={"guid": guid})

    # ---- status / tags / tasks ----

    def status(self) -> RSProjectStatus:
        """GET /project/status"""
        data = self._c._request("GET", "/project/status", require_session=True)
        return RSProjectStatus.from_json(data)

    def tags(self) -> List[str]:
        """GET /project/tags"""
        data = self._c._request("GET", "/project/tags", require_session=True)
        return list(data)

    def test_tag(self, tag: str) -> bool:
        """GET /project/testtag?tag=..."""
        data = self._c._request("GET", "/project/testtag", require_session=True, params={"tag": tag})
        return bool(data)

    def clear_tags(self, *, tag: Optional[str] = None) -> bool:
        """GET /project/cleartags?tag=... (optional)"""
        params = {"tag": tag} if tag else None
        data = self._c._request("GET", "/project/cleartags", require_session=True, params=params)
        return bool(data)

    def tasks(self, *, task_ids: Optional[Sequence[str]] = None) -> List[TaskStatus]:
        """GET /project/tasks?taskIDs=... (repeatable)"""
        params = self._c._array_params("taskIDs", task_ids)
        data = self._c._request("GET", "/project/tasks", require_session=True, params=params)
        return [TaskStatus.from_json(x) for x in data]

    def clear_tasks(self, *, task_ids: Optional[Sequence[str]] = None) -> None:
        """GET /project/cleartasks?taskIds=... (repeatable)"""
        params = self._c._array_params("taskIds", task_ids)
        self._c._request("GET", "/project/cleartasks", require_session=True, params=params or None)

    # ---- commands ----

    def command(
        self,
        name: str,
        *,
        params: Optional[Sequence[str]] = None,
        conditional_tag: Optional[str] = None,
        use_post: bool = False,
        encoded: Optional[str] = None,
        post_body: Optional[bytes] = None,
    ) -> TaskHandle:
        """
        GET  /project/command?name=...&param1=...&param2=...
        GET  /project/condcommand?tag=...&name=...&param1=...
        POST /project/command (same query params; body is raw or base64)
        POST /project/condcommand (same query params; body is raw or base64)

        Returns TaskHandle { taskID }.
        """
        q: Dict[str, Any] = {"name": name}
        if conditional_tag:
            q["tag"] = conditional_tag
            path = "/project/condcommand"
        else:
            path = "/project/command"

        if params:
            for i, p in enumerate(params[:9], start=1):
                q[f"param{i}"] = p

        if encoded:
            q["encoded"] = encoded

        method = "POST" if use_post else "GET"
        data = self._c._request(
            method,
            path,
            require_session=True,
            params=q,
            content=post_body if use_post else None,
        )
        return TaskHandle.from_json(data)

    def command_group(self, command_calls: Dict[str, Any]) -> TaskHandle:
        """
        POST /project/commandgroup
        Body: commandCall (the API docs show a structured body; keep it unopinionated here).
        """
        data = self._c._request(
            "POST",
            "/project/commandgroup",
            require_session=True,
            json=command_calls,
        )
        return TaskHandle.from_json(data)

    def cond_command_group(self, tag: str) -> TaskHandle:
        """POST /project/condcommandgroup?tag=..."""
        data = self._c._request(
            "POST",
            "/project/condcommandgroup",
            require_session=True,
            params={"tag": tag},
        )
        return TaskHandle.from_json(data)

    # ---- Wrapped Commands ----

    def headless(self) -> TaskHandle:
        """Hides user interface."""
        return self.command("headless")

    def hide_ui(self) -> TaskHandle:
        """Hide user interface."""
        return self.command("hideUI")

    def show_ui(self) -> TaskHandle:
        """Shows hidden user interaction."""
        return self.command("showUI")

    def new_scene(self) -> TaskHandle:
        """Create a new empty scene."""
        return self.command("newScene")

    def load(self, project_path: str, autosave_action: Optional[str] = None) -> TaskHandle:
        """
        Load an existing project.
        autosave_action: 'recoverAutosave' or 'deleteAutosave'
        """
        params = [project_path]
        if autosave_action:
            params.append(autosave_action)
        return self.command("load", params=params)

    def save(self, project_path: Optional[str] = None) -> TaskHandle:
        """Save the current project."""
        params = [project_path] if project_path else []
        return self.command("save", params=params)

    def start(self) -> TaskHandle:
        """
        Run the processes configured for the Start button.
        Adjust these settings in the Start button settings.
        """
        return self.command("start")

    def unlock_ppi_project(self, project_path: str) -> TaskHandle:
        """Save and unlock your PPI projects."""
        return self.command("unlockPPIProject", params=[project_path])

    def add_image(self, image_path: str) -> TaskHandle:
        """Import one or more images from a specified file path or .imagelist."""
        return self.command("add", params=[image_path])

    def add_folder(self, folder_path: str) -> TaskHandle:
        """Add all images in the specified folder."""
        return self.command("addFolder", params=[folder_path])

    def import_video(self, video_path: str, extracted_frames_path: str, jump_length: float) -> TaskHandle:
        """Import frames extracted from a video."""
        return self.command(
            "importVideo",
            params=[video_path, extracted_frames_path, str(jump_length)],
        )

    def import_leica_blk3d(self, file_path: str) -> TaskHandle:
        """Import an image sequence with .cmi extension captured by Leica BLK3D."""
        return self.command("importLeicaBlk3D", params=[file_path])

    def import_laser_scan(self, laser_scan_name: str, params_xml: Optional[str] = None) -> TaskHandle:
        """Add a LiDAR scan or a LiDAR-scan list."""
        params = [laser_scan_name]
        if params_xml:
            params.append(params_xml)
        return self.command("importLaserScan", params=params)

    def import_laser_scan_folder(self, folder_path: str, params_xml: Optional[str] = None) -> TaskHandle:
        """Add all LiDAR scans in the specified folder."""
        params = [folder_path]
        if params_xml:
            params.append(params_xml)
        return self.command("importLaserScanFolder", params=params)

    def import_hdr_images(self, source: str, params_xml: Optional[str] = None) -> TaskHandle:
        """Import HDR image, list of images or all images from a folder."""
        params = [source]
        if params_xml:
            params.append(params_xml)
        return self.command("importHDRimages", params=params)

    def add_image_with_calibration(self, image_path: str, xmp_path: str) -> TaskHandle:
        """Import an image as well as the corresponding XMP file."""
        return self.command("addImageWithCalibration", params=[image_path, xmp_path])

    def import_image_selection(self, file_path: str) -> TaskHandle:
        """Select scene images and/or LiDAR scans listed in a file."""
        return self.command("importImageSelection", params=[file_path])

    def select_image(self, pattern: str, mode: Optional[str] = None) -> TaskHandle:
        """
        Select a specified image or images defined by regex.
        mode: set, union, sub, intersect, toggle
        """
        params = [pattern]
        if mode:
            params.append(mode)
        return self.command("selectImage", params=params)

    def select_all_images(self) -> TaskHandle:
        """Select all images in the project."""
        return self.command("selectAllImages")

    def deselect_all_images(self) -> TaskHandle:
        """Deselect all images in the project."""
        return self.command("deselectAllImages")

    def invert_image_selection(self) -> TaskHandle:
        """Invert the current image selection."""
        return self.command("invertImageSelection")

    def remove_calibration_groups(self) -> TaskHandle:
        """Clear all inputs from their calibration groups."""
        return self.command("removeCalibrationGroups")

    def generate_ai_masks(self) -> TaskHandle:
        """Use AI Masking to generate masks."""
        return self.command("generateAIMasks")

    def export_masks(self, folder_path: str, params_xml: Optional[str] = None) -> TaskHandle:
        """Export the mask images currently used in the project."""
        params = [folder_path]
        if params_xml:
            params.append(params_xml)
        return self.command("exportMasks", params=params)

    def set_image_layer(self, index: int, image_path: str, layer_type: str) -> TaskHandle:
        """Set the layer from the image to the image defined with the index."""
        return self.command("setImageLayer", params=[str(index), image_path, layer_type])

    def set_images_layer(self, image_path: str, layer_type: str) -> TaskHandle:
        """Set the layer from the image to the selection of images."""
        return self.command("setImagesLayer", params=[image_path, layer_type])

    def remove_image_layer(self, layer_type: str) -> TaskHandle:
        """Remove the layers corresponding to the layerType from the selected images."""
        return self.command("removeImageLayer", params=[layer_type])

    def import_cache(self, folder_path: str) -> TaskHandle:
        """Import resource cache data from the specified folder."""
        return self.command("importCache", params=[folder_path])

    def clear_cache(self) -> TaskHandle:
        """Clear the application cache."""
        return self.command("clearCache")

    def exec_rscmd(self, command_file: str, args: Optional[Sequence[str]] = None) -> TaskHandle:
        """Execute commands from an .rscmd."""
        params = [command_file]
        if args:
            params.extend(args)
        return self.command("execRSCMD", params=params)

    def quit(self) -> TaskHandle:
        """Quit the application."""
        return self.command("quit")

    # ---- Delegate commands ----

    def set_instance_name(self, instance_name: str) -> TaskHandle:
        """Assign a name to a RealityScan instance."""
        return self.command("setInstanceName", params=[instance_name])

    def delegate_to(self, instance_name: str) -> TaskHandle:
        """Delegate a command to a specific instance."""
        return self.command("delegateTo", params=[instance_name])

    def wait_completed(self, instance_name: str) -> TaskHandle:
        """Pause execution until the process is finished in a specified instance."""
        return self.command("waitCompleted", params=[instance_name])

    def get_instance_status(self, instance_name: str) -> TaskHandle:
        """Return the progress status of a running process in a specified instance."""
        return self.command("getStatus", params=[instance_name])

    def pause_instance(self, instance_name: str) -> TaskHandle:
        """Pause a currently running process in a specified instance."""
        return self.command("pauseInstance", params=[instance_name])

    def unpause_instance(self, instance_name: str) -> TaskHandle:
        """Unpause a currently paused process in a specified instance."""
        return self.command("unpauseInstance", params=[instance_name])

    def abort_instance(self, instance_name: str) -> TaskHandle:
        """Abort a currently running process in a specified instance."""
        return self.command("abortInstance", params=[instance_name])

    def exec_rscmd_indirect(self, instance_name: str, command_file: str) -> TaskHandle:
        """Execute commands listed in the .rscmd file in the specified instance."""
        return self.command("execRSCMDIndirect", params=[instance_name, command_file])

    # ---- Selected Images Commands ----

    def set_feature_source(self, source: int) -> TaskHandle:
        """Define a feature source mode for the selected images (0, 1, 2)."""
        return self.command("setFeatureSource", params=[str(source)])

    def enable_alignment(self, enable: bool) -> TaskHandle:
        """Enable/disable selected images in the registration process."""
        return self.command("enableAlignment", params=["true" if enable else "false"])

    def enable_meshing(self, enable: bool) -> TaskHandle:
        """Enable/disable selected images in the model computation/meshing."""
        return self.command("enableMeshing", params=["true" if enable else "false"])

    def enable_texturing_and_coloring(self, enable: bool) -> TaskHandle:
        """Enable/disable selected images during the coloring and texture calculation."""
        return self.command("enableTexturingAndColoring", params=["true" if enable else "false"])

    def set_weight_in_texturing(self, weight: float) -> TaskHandle:
        """Set weight for selected images during the coloring and texture calculation <0,1>."""
        return self.command("setWeightInTexturing", params=[str(weight)])

    def enable_color_normalization_reference(self, enable: bool) -> TaskHandle:
        """Set selected images as color references in the color normalization process."""
        return self.command("enableColorNormalizationReference", params=["true" if enable else "false"])

    def enable_color_normalization(self, enable: bool) -> TaskHandle:
        """Enable or disable selected images in the color normalization process."""
        return self.command("enableColorNormalization", params=["true" if enable else "false"])

    def set_downscale_for_depth_maps(self, factor: int) -> TaskHandle:
        """Set a downscale factor for depth-map computation for the selected images."""
        return self.command("setDownscaleForDepthMaps", params=[str(factor)])

    def enable_in_component(self, enable: bool) -> TaskHandle:
        """Enable selected images in meshing and continue."""
        return self.command("enableInComponent", params=["true" if enable else "false"])

    def set_calibration_group_by_exif(self) -> TaskHandle:
        """Set the calibration group of all inputs based on their Exif."""
        return self.command("setCalibrationGroupByExif")

    def set_constant_calibration_groups(self) -> TaskHandle:
        """Group all selected inputs into a single calibration group."""
        return self.command("setConstantCalibrationGroups")

    def lock_pose_for_continue(self, lock: bool) -> TaskHandle:
        """Set relative camera pose unchanged for the selected images during the next registration."""
        return self.command("lockPoseForContinue", params=["true" if lock else "false"])

    def set_prior_calibration_group(self, group: int) -> TaskHandle:
        """Set a prior calibration group for the selected images (-1 or number)."""
        return self.command("setPriorCalibrationGroup", params=[str(group)])

    def set_prior_lens_group(self, group: int) -> TaskHandle:
        """Set a prior lens group for the selected images (-1 or number)."""
        return self.command("setPriorLensGroup", params=[str(group)])

    def edit_input_selection(self, settings: str) -> TaskHandle:
        """Edit the settings of the selected inputs based on the value in the Selected inputs panel or its key."""
        return self.command("editInputSelection", params=[settings])

    # ---- files ----

    def list_files(self, *, folder: Optional[str] = None) -> List[str]:
        """GET /project/list?folder=... (optional; output by default)"""
        params = {"folder": folder} if folder else None
        data = self._c._request("GET", "/project/list", require_session=True, params=params)
        return list(data)

    def download(self, name: str, *, folder: Optional[str] = None, mode: Optional[str] = None) -> bytes:
        """GET /project/download?name=...&folder=...&mode=... -> bytes"""
        params: Dict[str, Any] = {"name": name}
        if folder:
            params["folder"] = folder
        if mode:
            params["mode"] = mode
        data = self._c._request("GET", "/project/download", require_session=True, params=params)
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        # Fallback if server mislabels content-type
        return str(data).encode("utf-8")

    def upload(
        self,
        name: str,
        file_bytes: bytes,
        *,
        folder: Optional[str] = None,
        encoded: Optional[str] = None,
    ) -> None:
        """
        POST /project/upload?name=...&folder=...&encoded=...
        Body: raw bytes or base64 depending on encoded.
        """
        params: Dict[str, Any] = {"name": name}
        if folder:
            params["folder"] = folder
        if encoded:
            params["encoded"] = encoded
        self._c._request(
            "POST",
            "/project/upload",
            require_session=True,
            params=params,
            content=file_bytes,
        )

    def acknowledge_restart(self) -> None:
        """POST /project/acknowledgerestart"""
        self._c._request("POST", "/project/acknowledgerestart", require_session=True)
