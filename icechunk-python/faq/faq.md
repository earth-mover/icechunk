# Frequently Asked Questions

**Why do I have to opt-in to pickling an IcechunkStore or a Session?**

Icechunk is different from normal Zarr stores because it is stateful. In a distributed setting, you have to be careful to communicate back the Session objects from remote write tasks, merge them and commit them. The opt-in to pickle is a way for us to hint to the user that they need to be sure about what they are doing. We use pickling because these operations are only tricky once you cross a process boundary. More pragmatically, to_zarr(session.store) fails spectacularly in distributed contexts (e.g. [this issue](https://github.com/earth-mover/icechunk/issues/383)), and we do not want the user to be surprised.
