import React, { useEffect } from "react";
import { Provider, useDispatch } from "react-redux";
import KeplerGl from "kepler.gl";
import { addDataToMap, setMapStyle } from "kepler.gl/actions";
import store from "./store";
import { MAP_STYLE_OSM } from "./kepler-setup";

function Map() {
  const dispatch = useDispatch();

  useEffect(() => {
    // Postavi OSM mapu
    dispatch(setMapStyle(MAP_STYLE_OSM));

    // WebSocket konekcija ka backend-u
    const ws = new WebSocket("ws://kepler-backend:8000/ws"); // Docker mreža
    // Za lokalno testiranje: ws://localhost:8000/ws

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);

      // Pretvori podatke u Kepler dataset
      const keplerData = {
        info: { label: "Earthquakes", id: "earthquakes" },
        data: {
          fields: Object.keys(data).map((k) => ({
            name: k,
            type: typeof data[k] === "number" ? "real" : "string",
          })),
          rows: [Object.values(data)],
        },
      };

      dispatch(
        addDataToMap({
          datasets: keplerData, // ovde je bio problem sa "c"
          options: { centerMap: true, readOnly: false },
          config: {},
        })
      );
    };

    return () => ws.close();
  }, [dispatch]);

  return <KeplerGl id="kepler" width={window.innerWidth} height={window.innerHeight} />;
}

export default function App() {
  return (
    <Provider store={store}>
      <Map />
    </Provider>
  );
}
