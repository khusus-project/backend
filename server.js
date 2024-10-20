require("dotenv").config();
const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const { PassThrough } = require("stream");
const bodyParser = require("body-parser");

const app = express();
app.use(cors());
app.use(express.json());
app.use(bodyParser.urlencoded({ extended: true }));

const PORT = process.env.PORT || 3001;
const CSV_URL = process.env.CSV_URL;
const CSV_GRAFIK_DEMO = process.env.CSV_GRAFIK_DEMO;

// Fungsi untuk streaming data CSV dari URL
const streamCsvFromUrl = async () => {
  return new Promise((resolve, reject) => {
    const dataRows = [];

    axios({
      method: "get",
      url: CSV_URL,
      responseType: "stream",
    })
      .then((response) => {
        const csvStream = response.data.pipe(csv());
        csvStream
          .on("data", (row) => {
            dataRows.push(row);
          })
          .on("end", () => {
            resolve(dataRows); // Mengembalikan semua data setelah streaming selesai
          })
          .on("error", (error) => {
            reject(error);
          });
      })
      .catch((error) => {
        reject(error);
      });
  });
};

const streamCsvDemoFromUrl = async () => {
  return new Promise((resolve, reject) => {
    const dataRows = [];

    axios({
      method: "get",
      url: CSV_GRAFIK_DEMO,
      responseType: "stream",
    })
      .then((response) => {
        const csvStream = response.data.pipe(csv());
        csvStream
          .on("data", (row) => {
            dataRows.push(row);
          })
          .on("end", () => {
            resolve(dataRows); // Mengembalikan semua data setelah streaming selesai
          })
          .on("error", (error) => {
            reject(error);
          });
      })
      .catch((error) => {
        reject(error);
      });
  });
};

const csvDataSummary = async () => {
  return new Promise((resolve, reject) => {
    const dataRows = [];
    let totalDpt = 0;
    let uniqueDistrik = new Set();
    let uniqueKelurahan = new Set();
    let uniqueSlug = new Set();

    let ageCategories = {
      under17: { count: 0, percentage: 0, L: 0, P: 0 },
      b18sd25: { count: 0, percentage: 0, L: 0, P: 0 },
      b26sd35: { count: 0, percentage: 0, L: 0, P: 0 },
      b36sd55: { count: 0, percentage: 0, L: 0, P: 0 },
      b56sd60: { count: 0, percentage: 0, L: 0, P: 0 },
      over60: { count: 0, percentage: 0, L: 0, P: 0 },
    };

    axios({
      method: "get",
      url: CSV_URL,
      responseType: "stream",
    })
      .then((response) => {
        const csvStream = response.data.pipe(csv());
        csvStream
          .on("data", (row) => {
            const kecamatan = row["id_kecamatan"]; // Asumsi kolom Kecamatan
            const kelurahan = row["id_kelurahan"]; // Asumsi kolom Kelurahan
            const gender = row["gender"]; // Asumsi kolom Gender (L atau P)
            const age = parseInt(row["umur"]); // Asumsi kolom usia dinamakan 'usia'
            const slug = row["slug"]; // Asumsi kolom ID
            totalDpt++;

            if (slug) {
              uniqueSlug.add(slug);
            }

            if (kecamatan) {
              uniqueDistrik.add(kecamatan);
            }
            if (kelurahan) {
              uniqueKelurahan.add(kelurahan);
            }

            if (!isNaN(age)) {
              //   totalCount++; // Tambahkan total jumlah data

              // Kategorisasi usia
              if (age <= 17) {
                ageCategories.under17.count++;
                if (gender === "L") ageCategories.under17.L++;
                if (gender === "P") ageCategories.under17.P++;
              } else if (age >= 18 && age <= 25) {
                ageCategories["b18sd25"].count++;
                if (gender === "L") ageCategories["b18sd25"].L++;
                if (gender === "P") ageCategories["b18sd25"].P++;
              } else if (age >= 26 && age <= 35) {
                ageCategories["b26sd35"].count++;
                if (gender === "L") ageCategories["b26sd35"].L++;
                if (gender === "P") ageCategories["b26sd35"].P++;
              } else if (age >= 36 && age <= 55) {
                ageCategories["b36sd55"].count++;
                if (gender === "L") ageCategories["b36sd55"].L++;
                if (gender === "P") ageCategories["b36sd55"].P++;
              } else if (age >= 56 && age <= 60) {
                ageCategories["b56sd60"].count++;
                if (gender === "L") ageCategories["b56sd60"].L++;
                if (gender === "P") ageCategories["b56sd60"].P++;
              } else {
                ageCategories.over60.count++;
                if (gender === "L") ageCategories.over60.L++;
                if (gender === "P") ageCategories.over60.P++;
              }
            }
          })
          .on("end", () => {
            const jumlahDistrik = Array.from(uniqueDistrik).length;
            const jumlahKelurahan = Array.from(uniqueKelurahan).length;
            const jumlahTPS = Array.from(uniqueSlug).length;
            Object.keys(ageCategories).forEach((key) => {
              const totalInCategory = ageCategories[key].count;
              ageCategories[key].percentage = (
                (totalInCategory / totalDpt) *
                100
              ).toFixed(2); // Persentase
            });
            resolve({
              totalDpt,
              jumlahDistrik,
              jumlahKelurahan,
              jumlahTPS,
              ageCategories,
            });
          })
          .on("error", (error) => {
            reject(error); // Tangani error
          });
      })
      .catch((error) => {
        reject(error);
      });
  });
};

const csvTpsSummary = async () => {
  return new Promise((resolve, reject) => {
    const data = [];
    const summaryTps = [];

    const groupByTps = (row) => {
      const kelurahan = row.kelurahan;
      const kecamatan = row.kecamatan;
      const id_kelurahan = row.id_kelurahan;
      const id_kecamatan = row.id_kecamatan;
      const gender = row.gender;
      const slug = row.slug;
      const tps = row.tps;

      let existingTps = summaryTps.find((item) => item.slug === slug);

      if (!existingTps) {
        existingTps = {
          tps: tps,
          slug: slug,
          id_kecamatan: id_kecamatan,
          id_kelurahan: id_kelurahan,
          kecamatan: kecamatan,
          kelurahan: kelurahan,
          pria: 0,
          wanita: 0,
          totalDpt: 0,
        };
        summaryTps.push(existingTps);
      }

      if (gender === "L") {
        existingTps.pria += 1;
      } else if (gender === "P") {
        existingTps.wanita += 1;
      }
      existingTps.totalDpt++;
    };

    axios({
      method: "get",
      url: CSV_URL,
      responseType: "stream",
    })
      .then((response) => {
        const csvStream = response.data.pipe(csv());
        csvStream
          .on("data", (row) => {
            groupByTps(row);
          })
          .on("end", () => {
            resolve(summaryTps);
          })
          .on("error", (error) => {
            reject(error);
          });
      })
      .catch((error) => {
        reject(error);
      });
  });
};

const groupByKelurahan = (data) => {
  const summaryKelurahan = [];
  data.forEach((row) => {
    const id_kecamatan = row.id_kecamatan;
    const id_kelurahan = row.id_kelurahan;
    const kecamatan = row.kecamatan;
    const kelurahan = row.kelurahan;
    const pria = row.pria;
    const wanita = row.wanita;
    const totalDpt = row.totalDpt;

    let existingKelurahan = summaryKelurahan.find(
      (item) => item.id_kelurahan === id_kelurahan
    );

    if (!existingKelurahan) {
      existingKelurahan = {
        id_kecamatan: id_kecamatan,
        id_kelurahan: id_kelurahan,
        kecamatan: kecamatan,
        kelurahan: kelurahan,
        totalTps: 0,
        pria: 0,
        wanita: 0,
        totalDpt: 0,
      };
      summaryKelurahan.push(existingKelurahan);
    }
    existingKelurahan.pria += pria;
    existingKelurahan.wanita += wanita;
    existingKelurahan.totalDpt += totalDpt;
    existingKelurahan.totalTps += 1;
  });
  return summaryKelurahan;
};

const groupGrafikByKelurahan = (data) => {
  const perolehanKelurahan = [];
  data.forEach((row) => {
    const id_kecamatan = row.id_kecamatan;
    const id_kelurahan = row.id_kelurahan;
    const kecamatan = row.kecamatan;
    const kelurahan = row.kelurahan;
    const pria = Number(row.pria);
    const wanita = Number(row.wanita);
    const totalDpt = Number(row.totalDpt);
    const pekman = Number(row.pekman);
    const jbr_hadir = Number(row.jbr_hadir);
    const bmd_dipo = Number(row.bmd_dipo);
    const abr_harus = Number(row.abr_harus);

    let existingPerolehanKelurahan = perolehanKelurahan.find(
      (item) => item.id_kelurahan === id_kelurahan
    );

    if (!existingPerolehanKelurahan) {
      existingPerolehanKelurahan = {
        id_kecamatan: id_kecamatan,
        id_kelurahan: id_kelurahan,
        kecamatan: kecamatan,
        kelurahan: kelurahan,
        totalTps: 0,
        pria: 0,
        wanita: 0,
        totalDpt: 0,
        pekman: 0,
        jbr_hadir: 0,
        bmd_dipo: 0,
        abr_harus: 0,
      };
      perolehanKelurahan.push(existingPerolehanKelurahan);
    }
    existingPerolehanKelurahan.pria += pria;
    existingPerolehanKelurahan.wanita += wanita;
    existingPerolehanKelurahan.totalDpt += totalDpt;
    existingPerolehanKelurahan.totalTps += 1;
    existingPerolehanKelurahan.pekman += pekman;
    existingPerolehanKelurahan.jbr_hadir += jbr_hadir;
    existingPerolehanKelurahan.bmd_dipo += bmd_dipo;
    existingPerolehanKelurahan.abr_harus += abr_harus;
  });
  return perolehanKelurahan;
};

const groupGrafikByKecamatan = (data) => {
  const perolehanKecamatan = [];
  data.forEach((row) => {
    const id_kecamatan = row.id_kecamatan;
    const id_kelurahan = row.id_kelurahan;
    const kecamatan = row.kecamatan;
    const kelurahan = row.kelurahan;
    const pria = Number(row.pria);
    const wanita = Number(row.wanita);
    const totalDpt = Number(row.totalDpt);
    const pekman = Number(row.pekman);
    const jbr_hadir = Number(row.jbr_hadir);
    const bmd_dipo = Number(row.bmd_dipo);
    const abr_harus = Number(row.abr_harus);

    let existingPerolehanKecamatan = perolehanKecamatan.find(
      (item) => item.id_kecamatan === id_kecamatan
    );

    if (!existingPerolehanKecamatan) {
      existingPerolehanKecamatan = {
        id_kecamatan: id_kecamatan,
        id_kelurahan: id_kelurahan,
        kecamatan: kecamatan,
        kelurahan: kelurahan,
        totalTps: 0,
        pria: 0,
        wanita: 0,
        totalDpt: 0,
        pekman: 0,
        jbr_hadir: 0,
        bmd_dipo: 0,
        abr_harus: 0,
      };
      perolehanKecamatan.push(existingPerolehanKecamatan);
    }
    existingPerolehanKecamatan.pria += pria;
    existingPerolehanKecamatan.wanita += wanita;
    existingPerolehanKecamatan.totalDpt += totalDpt;
    existingPerolehanKecamatan.totalTps += 1;
    existingPerolehanKecamatan.pekman += pekman;
    existingPerolehanKecamatan.jbr_hadir += jbr_hadir;
    existingPerolehanKecamatan.bmd_dipo += bmd_dipo;
    existingPerolehanKecamatan.abr_harus += abr_harus;
  });
  return perolehanKecamatan;
};

const hasilPaslon = (data) => {
  const result = [
    {
      pekman: data.reduce((total, item) => total + Number(item.pekman), 0),
      slug: "PEKMAN",
    },
    {
      jbr_hadir: data.reduce(
        (total, item) => total + Number(item.jbr_hadir),
        0
      ),
      slug: "JBR HADIR",
    },
    {
      bmd_dipo: data.reduce((total, item) => total + Number(item.bmd_dipo), 0),
      slug: "BMD DIPO",
    },
    {
      abr_harus: data.reduce(
        (total, item) => total + Number(item.abr_harus),
        0
      ),
      slug: "ABR HARUS",
    },
  ];
  return result;
};

app.get("/get-summary", async (req, res) => {
  try {
    const summary = await csvDataSummary(); // Streaming CSV dari fungsi csvDataSummary
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/get-summary-tps", async (req, res) => {
  try {
    const summary = await csvTpsSummary(); // Streaming CSV dari fungsi csvDataSummary
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/get-summary-kelurahan", async (req, res) => {
  try {
    const data = await csvTpsSummary(); // Streaming CSV dari fungsi csvDataSummary
    const summary = await groupByKelurahan(data);
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/get-all-data", async (req, res) => {
  try {
    const data = await streamCsvFromUrl(); // Streaming CSV dari fungsi csvDataSummary
    res.json(data); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/get-grafik-demo-kecamatan", async (req, res) => {
  try {
    const data = await streamCsvDemoFromUrl(); // Streaming CSV dari fungsi csvDataSummary
    const summary = await groupGrafikByKecamatan(data);
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/get-hasil-paslon", async (req, res) => {
  try {
    const data = await streamCsvDemoFromUrl(); // Streaming CSV dari fungsi csvDataSummary
    const summary = await hasilPaslon(data);
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});
app.get("/get-grafik-demo-kelurahan", async (req, res) => {
  try {
    const data = await streamCsvDemoFromUrl(); // Streaming CSV dari fungsi csvDataSummary
    const summary = await groupGrafikByKelurahan(data);
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/", async (req, res) => {
  console.log("get from backend");
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
